//! Entry point for Terminal UI mode

use crossterm::event::{Event, KeyCode, KeyModifiers};
use futures::StreamExt as _;

use crate::Arg;
use crate::generic::task_tracker::{TaskExit, TaskTrackerMain, TrackedTaskResult};

mod render;

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

/// Terminal UI application
struct App {
    /// Indication that application shall run / exit
    running: bool,
}

impl App {
    /// Create the application
    #[allow(clippy::unused_async)]
    async fn new(_arg: Arg) -> anyhow::Result<Self> {
        Ok(Self { running: true })
    }

    /// Execute the application
    async fn task(mut self) -> TrackedTaskResult {
        let mut terminal = ratatui::init();
        let mut terminal_events = crossterm::event::EventStream::new().ready_chunks(16);
        let mut redraw = true;

        // TUI event loop
        while self.running {
            if redraw {
                terminal.draw(|frame| frame.render_widget(&self, frame.area()))?;
            }

            redraw = tokio::select! {
                Some(events) = terminal_events.next() => self.handle_terminal_event(events).await?,
            };
        }

        Ok(TaskExit::MainTaskStopAppSuccess)
    }

    /// Manage terminal events
    #[allow(clippy::unused_async)]
    async fn handle_terminal_event(
        &mut self,
        events: Vec<Result<Event, std::io::Error>>,
    ) -> anyhow::Result<bool> {
        let mut redraw = false;
        for event in events {
            let Ok(event) = event else {
                continue;
            };
            match event {
                Event::Key(key_event) => {
                    match key_event.code {
                        // quit
                        KeyCode::Char('q' | 'Q') => self.running = false,
                        KeyCode::Char('c' | 'C')
                            if key_event.modifiers == KeyModifiers::CONTROL =>
                        {
                            self.running = false;
                        }
                        _ => (), // ignored key
                    }
                }
                Event::Resize(_, _) => redraw = true,
                _ => (), // ignored event
            }

            if !self.running {
                // quit, no need to redraw
                return Ok(false);
            }
        }
        Ok(redraw)
    }
}
