mod model;
mod net;
mod text_field;
mod theme;

use gpui::{AppContext, Focusable, prelude::*, rgb,
    App, Bounds, Context, Entity, IntoElement, KeyBinding, MouseButton, Render,
    SharedString, Window, WindowBounds, WindowOptions, div, px, size,
};
use gpui_platform::application;
use model::{AppState, ConnectionStatus, Role};
use net::{ClientCommand, ServerEndpoint, ShellEvent};
use text_field::{PromptSubmitted, TextField};

struct AppView {
    state: AppState,
    composer: Entity<TextField>,
    commands: Option<std::sync::mpsc::Sender<ClientCommand>>,
}

impl AppView {
    fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let composer = cx.new(|cx| TextField::new("Message the agent…", cx));
        composer.read(cx).focus_handle(cx).focus(window, cx);
        cx.subscribe_in(
            &composer,
            window,
            |this, _composer, event: &PromptSubmitted, _window, cx| {
                this.on_prompt_submitted(&event.0, cx);
            },
        )
        .detach();

        let mut commands = None;
        let state = match ServerEndpoint::from_env() {
            Some(endpoint) => {
                let (events_tx, events_rx) = smol::channel::unbounded::<ShellEvent>();
                let (commands_tx, commands_rx) = std::sync::mpsc::channel::<ClientCommand>();
                std::thread::spawn(move || net::run(endpoint, events_tx, commands_rx));
                commands = Some(commands_tx);

                cx.spawn(async move |this, cx| {
                    while let Ok(event) = events_rx.recv().await {
                        let alive = this.update(cx, |this, cx| {
                            this.apply_shell_event(event, cx);
                        });
                        if alive.is_err() {
                            break;
                        }
                    }
                })
                .detach();

                AppState::connecting()
            }
            None => AppState::mock(),
        };

        Self {
            state,
            composer,
            commands,
        }
    }

    fn apply_shell_event(&mut self, event: ShellEvent, cx: &mut Context<Self>) {
        match event {
            ShellEvent::Connected => {
                self.state.connection = ConnectionStatus::Connected;
            }
            ShellEvent::Snapshot(snapshot) => {
                self.state.projects = snapshot
                    .projects
                    .into_iter()
                    .map(|p| model::Project {
                        id: p.id.into(),
                        name: p.title.into(),
                        path: p.workspace_root.into(),
                        expanded: true,
                    })
                    .collect();
                self.state.threads = snapshot
                    .threads
                    .into_iter()
                    .filter(|t| !t.deleted && !t.archived)
                    .map(|t| model::Thread {
                        id: t.id.into(),
                        project_id: t.project_id.into(),
                        title: t.title.into(),
                        status: t.status.into(),
                    })
                    .collect();
                if self.state.selected.is_none() {
                    self.state.selected =
                        self.state.threads.first().map(|t| t.id.clone());
                }
            }
            ShellEvent::ProjectUpserted(p) => {
                let project = model::Project {
                    id: p.id.clone().into(),
                    name: p.title.into(),
                    path: p.workspace_root.into(),
                    expanded: true,
                };
                match self.state.projects.iter().position(|e| e.id.as_ref() == p.id) {
                    Some(i) => self.state.projects[i] = project,
                    None => self.state.projects.push(project),
                }
            }
            ShellEvent::ProjectRemoved { project_id } => {
                self.state.projects.retain(|p| p.id.as_ref() != project_id);
            }
            ShellEvent::ThreadUpserted { thread } => {
                if thread.deleted || thread.archived {
                    self.state.threads.retain(|t| t.id.as_ref() != thread.id);
                } else {
                    let entry = model::Thread {
                        id: thread.id.clone().into(),
                        project_id: thread.project_id.into(),
                        title: thread.title.into(),
                        status: thread.status.into(),
                    };
                    match self.state.threads.iter().position(|e| e.id.as_ref() == thread.id) {
                        Some(i) => self.state.threads[i] = entry,
                        None => self.state.threads.push(entry),
                    }
                }
            }
            ShellEvent::ThreadRemoved { thread_id, .. } => {
                self.state.threads.retain(|t| t.id.as_ref() != thread_id);
            }
            ShellEvent::Error(e) => {
                self.state.connection = ConnectionStatus::Failed(e.into());
            }
            ShellEvent::Disconnected(reason) => {
                self.state.connection = ConnectionStatus::Disconnected(reason.into());
            }
        }
        cx.notify();
    }

    fn on_prompt_submitted(&mut self, text: &SharedString, cx: &mut Context<Self>) {
        self.state.messages.push(model::Message {
            role: Role::User,
            text: text.clone(),
        });

        if self.state.connection == ConnectionStatus::Connected {
            let thread_id = match self.state.selected_thread() {
                Some(thread) => thread.id.to_string(),
                None => {
                    // No thread selected: create one under the first project.
                    let Some(project) = self.state.projects.first() else {
                        self.state.messages.push(model::Message {
                            role: Role::Agent,
                            text: "No project on this server to create a thread in.".into(),
                        });
                        cx.notify();
                        return;
                    };
                    let thread_id = uuid::Uuid::new_v4().to_string();
                    self.dispatch(
                        serde_json::json!({
                            "type": "thread.create",
                            "commandId": uuid::Uuid::new_v4().to_string(),
                            "threadId": thread_id,
                            "projectId": project.id.to_string(),
                            "title": text.chars().take(60).collect::<String>(),
                            "modelSelection": { "instanceId": "devReplay", "model": "showcase" },
                            "runtimeMode": "full-access",
                            "interactionMode": "default",
                            "branch": null,
                            "worktreePath": null,
                            "createdBy": "user",
                            "creationSource": "web",
                        }),
                    );
                    self.state.selected = Some(thread_id.clone().into());
                    thread_id
                }
            };

            self.dispatch(
                serde_json::json!({
                    "type": "message.dispatch",
                    "commandId": uuid::Uuid::new_v4().to_string(),
                    "threadId": thread_id,
                    "messageId": uuid::Uuid::new_v4().to_string(),
                    "text": text.to_string(),
                    "attachments": [],
                    "dispatchMode": { "type": "start_immediately" },
                    "createdBy": "user",
                    "creationSource": "web",
                }),
            );
        } else {
            self.state.messages.push(model::Message {
                role: Role::Agent,
                text: "Received — the Rust client is not wired to a 67 server yet.".into(),
            });
        }
        cx.notify();
    }

    fn dispatch(&mut self, command: serde_json::Value) {
        if let Some(commands) = &self.commands {
            let _ = commands.send(ClientCommand::Dispatch { command });
        }
    }

    fn select_thread(&mut self, thread_id: SharedString, cx: &mut Context<Self>) {
        if let Some(commands) = &self.commands {
            let _ = commands.send(ClientCommand::SubscribeThread {
                thread_id: thread_id.to_string(),
            });
        }
        self.state.selected = Some(thread_id);
        cx.notify();
    }

    fn render_sidebar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let mut projects = div().flex().flex_col().flex_1().overflow_y_hidden().px_2();
        for (pi, project) in self.state.projects.iter().enumerate() {
            let mut section = div()
                .flex()
                .flex_col()
                .child(
                    div()
                        .id(("project", pi))
                        .px_2()
                        .py_1p5()
                        .rounded_md()
                        .text_sm()
                        .font_weight(gpui::FontWeight::MEDIUM)
                        .text_color(theme::fg())
                        .hover(|s| s.bg(theme::hover()))
                        .child(project.name.clone()),
                )
                .child(
                    div()
                        .px_2()
                        .pb_1()
                        .text_xs()
                        .text_color(theme::fg_muted())
                        .child(project.path.clone()),
                );
            if project.expanded {
                for (ti, thread) in self.state.threads_for(&project.id).enumerate() {
                    let active = self.state.selected.as_ref() == Some(&thread.id);
                    let thread_id = thread.id.clone();
                    let mut row = div()
                        .id(("thread", pi * 10_000 + ti))
                        .mx_1()
                        .px_2()
                        .py_1()
                        .rounded_md()
                        .text_sm()
                        .when(active, |s| s.bg(theme::selected()).text_color(theme::fg()))
                        .when(!active, |s| {
                            s.text_color(theme::fg_muted())
                                .hover(|s| s.bg(theme::hover()))
                        })
                        .on_mouse_down(
                            MouseButton::Left,
                            cx.listener(move |this, _ev, _window, cx| {
                                this.select_thread(thread_id.clone(), cx)
                            }),
                        )
                        .child(thread.title.clone());
                    if thread.status.as_ref() != "idle" {
                        row = row.child(
                            div()
                                .ml_1()
                                .text_xs()
                                .text_color(theme::primary())
                                .child(format!("({})", thread.status)),
                        );
                    }
                    section = section.child(row);
                }
            }
            projects = projects.child(section);
        }

        let status_text = match &self.state.connection {
            ConnectionStatus::Mock => "Local mock data".to_string(),
            ConnectionStatus::Connecting => "Connecting…".to_string(),
            ConnectionStatus::Connected => "Connected".to_string(),
            ConnectionStatus::Failed(e) => format!("Error: {e}"),
            ConnectionStatus::Disconnected(e) => format!("Disconnected: {e}"),
        };
        let status_color = match &self.state.connection {
            ConnectionStatus::Connected => theme::primary(),
            ConnectionStatus::Failed(_) | ConnectionStatus::Disconnected(_) => rgb(0xff5a5a),
            _ => theme::fg_muted(),
        };

        div()
            .w(px(260.))
            .h_full()
            .flex()
            .flex_col()
            .bg(theme::sidebar())
            .border_r_1()
            .border_color(theme::border())
            .child(
                div()
                    .h(px(44.))
                    .px_4()
                    .flex()
                    .items_center()
                    .border_b_1()
                    .border_color(theme::border())
                    .child(
                        div()
                            .text_lg()
                            .font_weight(gpui::FontWeight::BOLD)
                            .text_color(theme::fg())
                            .child("67"),
                    ),
            )
            .child(projects)
            .child(
                div()
                    .px_4()
                    .py_2()
                    .border_t_1()
                    .border_color(theme::border())
                    .text_xs()
                    .text_color(status_color)
                    .child(status_text),
            )
    }

    fn render_messages(&self) -> impl IntoElement {
        let mut list = div()
            .id("messages")
            .flex()
            .flex_col()
            .flex_1()
            .overflow_y_scroll()
            .p_4()
            .gap_3();

        if self.state.messages.is_empty() {
            return list.child(
                div()
                    .flex_1()
                    .flex()
                    .items_center()
                    .justify_center()
                    .text_color(theme::fg_muted())
                    .text_sm()
                    .child("No messages yet — say something to the agent."),
            );
        }

        for (i, message) in self.state.messages.iter().enumerate() {
            let bubble = div()
                .px_3()
                .py_2()
                .rounded_lg()
                .text_sm()
                .max_w(px(560.))
                .child(message.text.clone());
            let row = match message.role {
                Role::User => div().flex().justify_end().child(
                    bubble.bg(theme::user_bubble()).text_color(theme::fg()),
                ),
                Role::Agent => div().flex().justify_start().child(
                    bubble
                        .bg(theme::raised())
                        .border_1()
                        .border_color(theme::border())
                        .text_color(theme::fg()),
                ),
            };
            list = list.child(div().id(("msg", i)).child(row));
        }
        list
    }

    fn render_composer(&self) -> impl IntoElement {
        div().p_4().child(
            div()
                .flex()
                .items_center()
                .gap_2()
                .rounded_lg()
                .bg(theme::raised())
                .border_1()
                .border_color(theme::border())
                .px_3()
                .py_2()
                .child(div().flex_1().text_sm().child(self.composer.clone()))
                .child(
                    div()
                        .id("send")
                        .px_3()
                        .py_1()
                        .rounded_md()
                        .bg(theme::primary())
                        .text_xs()
                        .text_color(gpui::white())
                        .child("Send"),
                ),
        )
    }
}

impl Render for AppView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let title = self
            .state
            .selected_thread()
            .map(|t| t.title.clone())
            .unwrap_or_else(|| "67 Code".into());
        div()
            .flex()
            .size_full()
            .bg(theme::canvas())
            .text_color(theme::fg())
            .child(self.render_sidebar(cx))
            .child(
                div()
                    .flex()
                    .flex_col()
                    .flex_1()
                    .h_full()
                    .child(
                        div()
                            .h(px(44.))
                            .px_4()
                            .flex()
                            .items_center()
                            .border_b_1()
                            .border_color(theme::border())
                            .text_sm()
                            .font_weight(gpui::FontWeight::MEDIUM)
                            .child(title),
                    )
                    .child(self.render_messages())
                    .child(self.render_composer()),
            )
    }
}

fn main() {
    application().run(|cx: &mut App| {
        use text_field::*;
        cx.bind_keys([
            KeyBinding::new("backspace", Backspace, Some("TextField")),
            KeyBinding::new("delete", Delete, Some("TextField")),
            KeyBinding::new("left", Left, Some("TextField")),
            KeyBinding::new("right", Right, Some("TextField")),
            KeyBinding::new("shift-left", SelectLeft, Some("TextField")),
            KeyBinding::new("shift-right", SelectRight, Some("TextField")),
            KeyBinding::new("ctrl-a", SelectAll, Some("TextField")),
            KeyBinding::new("ctrl-v", Paste, Some("TextField")),
            KeyBinding::new("ctrl-c", Copy, Some("TextField")),
            KeyBinding::new("ctrl-x", Cut, Some("TextField")),
            KeyBinding::new("home", Home, Some("TextField")),
            KeyBinding::new("end", End, Some("TextField")),
            KeyBinding::new("enter", Submit, Some("TextField")),
        ]);
        let bounds = Bounds::centered(None, size(px(1200.0), px(760.0)), cx);
        cx.open_window(
            WindowOptions {
                titlebar: Some(gpui::TitlebarOptions {
                    title: Some("67 Code".into()),
                    ..Default::default()
                }),
                window_bounds: Some(WindowBounds::Windowed(bounds)),
                ..Default::default()
            },
            |window, cx| cx.new(|cx| AppView::new(window, cx)),
        )
        .unwrap();
        cx.activate(true);
    });
}
