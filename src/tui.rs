//! Entry point for Terminal UI mode

// allow cast for screen size (u16)
#![allow(clippy::cast_possible_truncation)]

use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
use futures::StreamExt as _;

use crate::diff::{DiffEntry, DiffMode};
use crate::generic::task_tracker::{TaskExit, TaskTracker, TaskTrackerMain, TrackedTaskResult};
use crate::sync_exec::SyncStat;
use crate::sync_plan::SyncMode;
use crate::{Arg, RunContext};

use list_panel::{ListPanelMove, ListPanelSelection};
use theme::AppTheme;

mod help;
mod list_panel;
mod render;
mod rich_text;
mod scroll_bar;
mod theme;

/// Terminal UI entry point
///
/// # Errors
/// - invalid arguments
/// - cannot create trees
pub async fn async_main_tui(arg: Arg) -> anyhow::Result<std::process::ExitCode> {
    anyhow::ensure!(
        arg.dirs.len() >= 2,
        "TUI mode: expects at least 2 directories"
    );

    let (exit_message_sender, exit_message_receiver) = flume::bounded(1);
    let theme = AppTheme::load(None)?;

    let task_tracker_main = TaskTrackerMain::default();
    task_tracker_main.setup_signal_catching()?;
    let app = App::new(task_tracker_main.tracker(), arg, exit_message_sender, theme);
    task_tracker_main.spawn(app.task())?;
    let result = task_tracker_main.wait().await;
    ratatui::restore();
    let result = result?;

    // get exit message if any
    if let Ok(exit_message) = exit_message_receiver.try_recv() {
        eprintln!("{exit_message}");
    }

    Ok(result)
}

/// Screen currently displayed
#[derive(PartialEq, Debug, Clone, Copy)]
enum Screen {
    /// Normal screen, diff/sync view
    Normal,
    /// Help screen
    Help,
    /// Confirm exit without syncing
    ConfirmExit,
}

/// Current view displayed
#[derive(PartialEq, Debug, Clone, Copy)]
enum View {
    /// Initial view: wait for end of browsing trees
    Browsing,
    /// Diff view, read only mode
    Diff,
    /// Sync: view all differences
    SyncAll,
    /// Sync: view conflicts
    SyncConflicts,
    /// Sync: view resolved differences
    SyncResolved,
    /// Exit view: wait for end of sync actions
    Syncing,
}
impl View {
    fn is_diff(&self) -> bool {
        *self == Self::Diff
    }

    fn are_diffs_rendered(&self) -> bool {
        *self == Self::Diff
            || *self == Self::SyncAll
            || *self == Self::SyncConflicts
            || *self == Self::SyncResolved
    }
}

/// Background task event towards app event loop
enum AppTaskEvent {
    /// Context determined by `init_task`
    InitContext(InitContext),
    /// Sync completed by `sync_task`
    SyncCompleted(SyncStat),
}

/// Context for tree and diffs, transferred between `init_task` and app
struct InitContext {
    run_ctx: RunContext,
    diffs: Vec<DiffEntry>,
}

/// Application runtime context (displaying diffs)
struct Context {
    run_ctx: RunContext,
    diffs: Vec<DiffEntry>,
    /// Diff list panel
    diff_list: ListPanelSelection,
}

/// Terminal UI application
struct App {
    task_tracker: TaskTracker,
    /// CLI arguments
    arg: Arg,
    /// Terminal UI theme
    theme: AppTheme,
    /// Indication that application shall run / exit
    running: bool,
    /// Redraw required on next event loop
    redraw: bool,
    /// Current screen displayed
    screen: Screen,
    /// Current view displayed
    view: View,
    /// Whether synchronization has been done
    sync_done: bool,
    /// Help content, filled on first display of the help screen
    help: Option<help::Help>,
    /// Send an exit message to the user (after terminal restore)
    exit_message_sender: flume::Sender<String>,
    /// Events from background task
    task_event_receiver: flume::Receiver<AppTaskEvent>,
    task_event_sender: flume::Sender<AppTaskEvent>,
    /// Runtime  context
    context: Option<Context>,
}

impl App {
    /// Create the application
    fn new(
        task_tracker: TaskTracker,
        mut arg: Arg,
        exit_message_sender: flume::Sender<String>,
        theme: AppTheme,
    ) -> Self {
        // use read-only mode if invoked as 'dir-diff'
        let invocation_name = std::env::args().next().expect("cannot get argv[0]");
        if invocation_name.ends_with("dir-diff") {
            arg.read_only = true;
        }

        let (task_event_sender, task_event_receiver) = flume::bounded(1);

        Self {
            task_tracker,
            arg,
            theme,
            running: true,
            redraw: true,
            screen: Screen::Normal,
            view: View::Browsing,
            sync_done: false,
            help: None,
            exit_message_sender,
            task_event_receiver,
            task_event_sender,
            context: None,
        }
    }

    /// Init background task
    ///
    /// Perform the steps of `sync_main`
    /// - create `RunContext` to spawn all trees
    /// - wait for tree walk completion
    /// - perform diff
    /// - determine sync plan if applicable
    /// - return `RunContext` + diff to App
    async fn init_task(
        task_tracker: TaskTracker,
        arg: Arg,
        task_event_sender: flume::Sender<AppTaskEvent>,
    ) -> TrackedTaskResult {
        // spawn all trees
        let mut run_ctx = RunContext::new(&task_tracker, &arg, !arg.read_only).await?;

        // wait for tree walk completion
        for tree in &mut run_ctx.trees {
            tree.wait_for_tree().await?;
        }

        // perform diff
        let mut diffs = {
            let root_entries = run_ctx.get_root_entries()?;
            crate::diff::diff_trees(
                task_tracker.clone(),
                &root_entries.as_ref(),
                DiffMode::Output,
            )?
        };

        if !arg.read_only {
            // get prev_snaps from all trees
            let prev_sync_snaps = run_ctx.get_prev_sync_snaps();

            // determine sync plan
            let sync_mode = if arg.latest {
                SyncMode::Latest
            } else {
                SyncMode::Standard
            };
            crate::sync_plan::sync_plan(task_tracker, prev_sync_snaps, sync_mode, &mut diffs)?;
        }

        task_event_sender.send(AppTaskEvent::InitContext(InitContext { run_ctx, diffs }))?;

        Ok(TaskExit::SecondaryTaskKeepRunning)
    }

    /// Background task: perform synchronization operations
    async fn sync_task(
        task_tracker: TaskTracker,
        task_event_sender: flume::Sender<AppTaskEvent>,
        mut run_ctx: RunContext,
        diffs: Vec<DiffEntry>,
    ) -> TrackedTaskResult {
        let sync_stat =
            crate::sync_exec::sync_exec(task_tracker, &mut run_ctx.trees, &diffs).await?;
        run_ctx.save_snaps_and_terminate(true).await;

        task_event_sender.send(AppTaskEvent::SyncCompleted(sync_stat))?;

        Ok(TaskExit::SecondaryTaskKeepRunning)
    }

    /// Execute the application
    async fn task(mut self) -> TrackedTaskResult {
        // spawn init task
        self.task_tracker.spawn(Self::init_task(
            self.task_tracker.clone(),
            self.arg.clone(),
            self.task_event_sender.clone(),
        ))?;

        let mut terminal = ratatui::init();
        let mut terminal_events = crossterm::event::EventStream::new().ready_chunks(16);

        // TUI event loop
        while self.running {
            if self.redraw {
                terminal.draw(|frame| frame.render_widget(&mut self, frame.area()))?;
                self.redraw = false;
            }

            tokio::select! {
                Some(events) = terminal_events.next() => self.handle_terminal_event(events),
                Ok(event) = self.task_event_receiver.recv_async() => self.handle_task_event(event),
            };
        }

        if let Some(mut context) = self.context.take() {
            context
                .run_ctx
                .save_snaps_and_terminate(self.sync_done)
                .await;
        }

        Ok(TaskExit::MainTaskStopAppSuccess)
    }

    /// Manage background task event
    fn handle_task_event(&mut self, event: AppTaskEvent) {
        match event {
            AppTaskEvent::InitContext(context) => {
                let no_diff = context.diffs.is_empty();
                // TODO: let diff_list = ListPanelSelection::new(context.diffs.len());
                let diff_list =
                    ListPanelSelection::new(10_usize.pow(self.arg.dirs.len() as u32 - 1));
                self.context = Some(Context {
                    run_ctx: context.run_ctx,
                    diffs: context.diffs,
                    diff_list,
                });
                if no_diff {
                    if !self.arg.read_only {
                        self.sync_done = true;
                    }
                    self.exit("No difference found between inputs".to_owned());
                    return;
                }
                self.set_view(if self.arg.read_only {
                    View::Diff
                } else {
                    View::SyncAll
                });
            }
            AppTaskEvent::SyncCompleted(sync_stat) => {
                self.sync_done = true;
                self.exit(format!(
                    concat!(
                        "Synchronization completed successfully:\n",
                        "- {} files synchronized\n",
                        "- {} conflicts remaining"
                    ),
                    sync_stat.sync_files, sync_stat.conflict_files
                ));
            }
        }
    }

    /// Manage terminal events
    fn handle_terminal_event(&mut self, events: Vec<Result<Event, std::io::Error>>) {
        for event in events {
            let Ok(event) = event else {
                // ignore error events
                continue;
            };
            match event {
                Event::Key(key_event) => {
                    if (key_event.code == KeyCode::Char('c')
                        || key_event.code == KeyCode::Char('C'))
                        && key_event.modifiers.contains(KeyModifiers::CONTROL)
                    {
                        // Ctrl+C is force exit
                        self.running = false;
                    } else {
                        match self.screen {
                            Screen::Normal => {
                                self.handle_key_event_screen_normal(key_event);
                            }
                            Screen::Help => {
                                // any key => leave help and return to normal screen
                                self.set_screen(Screen::Normal);
                            }
                            Screen::ConfirmExit => match key_event.code {
                                KeyCode::Char('q' | 'Q') => {
                                    // confirm exit without syncing
                                    self.running = false;
                                }
                                KeyCode::Char('c' | 'C') | KeyCode::Esc => {
                                    // cancel exit, return to normal screen
                                    self.set_screen(Screen::Normal);
                                }
                                KeyCode::Char('s' | 'S') => {
                                    // do sync operations
                                    self.set_screen(Screen::Normal);
                                    self.start_sync();
                                }
                                _ => {} // ignored key, user must choose
                            },
                        }
                    }
                }
                Event::Resize(_, _) => self.redraw = true,
                _ => (), // ignored event
            }

            if !self.running {
                // quit, no need to process the remaining events
                break;
            }
        }
    }

    /// Manage key events for normal screen
    fn handle_key_event_screen_normal(&mut self, key_event: KeyEvent) {
        match key_event.code {
            // quit
            KeyCode::Char('q' | 'Q') | KeyCode::Esc if self.view.are_diffs_rendered() => {
                if self.view.is_diff() {
                    // quit without confirmation
                    self.running = false;
                } else {
                    self.set_screen(Screen::ConfirmExit);
                }
            }
            KeyCode::Char('h' | 'H' | '?') | KeyCode::F(1) => {
                self.set_screen(Screen::Help);
            }
            KeyCode::F(5) if self.view.are_diffs_rendered() && !self.view.is_diff() => {
                self.set_view(View::SyncAll);
            }
            KeyCode::F(6) if self.view.are_diffs_rendered() && !self.view.is_diff() => {
                self.set_view(View::SyncConflicts);
            }
            KeyCode::F(7) if self.view.are_diffs_rendered() && !self.view.is_diff() => {
                self.set_view(View::SyncResolved);
            }
            KeyCode::Char('s' | 'S') if self.view.are_diffs_rendered() && !self.view.is_diff() => {
                // do sync operations
                self.start_sync();
            }
            KeyCode::Home => self.handle_key_diff_nav(ListPanelMove::Start),
            KeyCode::End => self.handle_key_diff_nav(ListPanelMove::End),
            KeyCode::PageUp => self.handle_key_diff_nav(ListPanelMove::PageUp),
            KeyCode::PageDown => self.handle_key_diff_nav(ListPanelMove::PageDown),
            KeyCode::Up => self.handle_key_diff_nav(ListPanelMove::LineUp),
            KeyCode::Down => self.handle_key_diff_nav(ListPanelMove::LineDown),
            _ => (), // ignored key
        }
    }

    /// Manage navigation keys for diff list
    fn handle_key_diff_nav(&mut self, event: ListPanelMove) {
        if let Some(context) = &mut self.context {
            context.diff_list.handle(event);
            self.redraw = true;
        }
    }

    /// Change screen to be displayed
    fn set_screen(&mut self, screen: Screen) {
        self.screen = screen;
        self.redraw = true;
    }

    /// Change view to be displayed
    fn set_view(&mut self, view: View) {
        if view != self.view {
            self.view = view;
            self.redraw = true;
        }
    }

    /// Start the synchronization process
    fn start_sync(&mut self) {
        let ctx = self.context.take().unwrap();
        let _ignored = self.task_tracker.spawn(Self::sync_task(
            self.task_tracker.clone(),
            self.task_event_sender.clone(),
            ctx.run_ctx,
            ctx.diffs,
        ));
        self.set_view(View::Syncing);
    }

    /// exit with message
    fn exit(&mut self, msg: String) {
        self.running = false;
        let _ignored = self.exit_message_sender.send(msg);
    }
}
