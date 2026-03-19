//! Entry point for Terminal UI mode

// allow cast for screen size (u16)
#![allow(clippy::cast_possible_truncation)]

use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
use futures::StreamExt as _;

use crate::Arg;
use crate::generic::task_tracker::{TaskExit, TaskTrackerMain, TrackedTaskResult};

use theme::AppTheme;

mod help;
mod render;
mod rich_text;
mod theme;

/// Terminal UI entry point
///
/// # Errors
/// - invalid arguments
/// - cannot create trees
pub async fn async_main_tui(arg: Arg) -> anyhow::Result<std::process::ExitCode> {
    let app = App::new(arg).await?;
    let task_tracker_main = TaskTrackerMain::default();
    task_tracker_main.setup_signal_catching()?;
    task_tracker_main.spawn(app.task())?;
    let result = task_tracker_main.wait().await;
    ratatui::restore();
    result
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
    /// Diff view, read only mode
    Diff,
    /// Sync: view all differences
    SyncAll,
    /// Sync: view conflicts
    SyncConflicts,
    /// Sync: view resolved differences
    SyncResolved,
}
impl View {
    fn is_diff(&self) -> bool {
        matches!(self, Self::Diff)
    }
}

/// Terminal UI application
struct App {
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
    /// Help content, filled on first display of the help screen
    help: Option<help::Help>,
}

impl App {
    /// Create the application
    #[allow(clippy::unused_async)]
    async fn new(mut arg: Arg) -> anyhow::Result<Self> {
        // use read-only mode if invoked as 'dir-diff'
        let invocation_name = std::env::args().next().expect("cannot get argv[0]");
        if invocation_name.ends_with("dir-diff") {
            arg.read_only = true;
        }

        let view = if arg.read_only {
            View::Diff
        } else {
            View::SyncAll
        };

        let theme = AppTheme::load(None)?;
        Ok(Self {
            theme,
            running: true,
            redraw: true,
            screen: Screen::Normal,
            view,
            help: None,
        })
    }

    /// Execute the application
    async fn task(mut self) -> TrackedTaskResult {
        let mut terminal = ratatui::init();
        let mut terminal_events = crossterm::event::EventStream::new().ready_chunks(16);

        // TUI event loop
        while self.running {
            if self.redraw {
                terminal.draw(|frame| frame.render_widget(&mut self, frame.area()))?;
                self.redraw = false;
            }

            tokio::select! {
                Some(events) = terminal_events.next() => self.handle_terminal_event(events).await?,
            };
        }

        Ok(TaskExit::MainTaskStopAppSuccess)
    }

    /// Manage terminal events
    async fn handle_terminal_event(
        &mut self,
        events: Vec<Result<Event, std::io::Error>>,
    ) -> anyhow::Result<()> {
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
                                self.handle_key_event_screen_normal(key_event).await?;
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
                                    anyhow::bail!("TODO: implement sync");
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
        Ok(())
    }

    #[allow(clippy::unused_async)]
    async fn handle_key_event_screen_normal(&mut self, key_event: KeyEvent) -> anyhow::Result<()> {
        match key_event.code {
            // quit
            KeyCode::Char('q' | 'Q') | KeyCode::Esc => {
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
            KeyCode::F(5) if !self.view.is_diff() => {
                self.set_view(View::SyncAll);
            }
            KeyCode::F(6) if !self.view.is_diff() => {
                self.set_view(View::SyncConflicts);
            }
            KeyCode::F(7) if !self.view.is_diff() => {
                self.set_view(View::SyncResolved);
            }
            KeyCode::Char('s' | 'S') => {
                // do sync operations
                anyhow::bail!("TODO: implement sync");
            }
            _ => (), // ignored key
        }
        Ok(())
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
}
