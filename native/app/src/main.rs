mod model;
mod text_field;
mod theme;

use gpui::{AppContext, prelude::*,
    App, Bounds, Context, Entity, IntoElement, KeyBinding, MouseButton, ParentElement, Render,
    SharedString, Styled, Window, WindowBounds, WindowOptions, div, px, size,
};
use gpui_platform::application;
use model::{AppState, Role};
use text_field::{PromptSubmitted, TextField};

struct AppView {
    state: AppState,
    composer: Entity<TextField>,
}

impl AppView {
    fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let composer = cx.new(|cx| TextField::new("Message the agent…", cx));
        cx.subscribe_in(
            &composer,
            window,
            |this, _composer, event: &PromptSubmitted, _window, cx| {
                this.on_prompt_submitted(&event.0, cx);
            },
        )
        .detach();
        Self {
            state: AppState::mock(),
            composer,
        }
    }

    fn on_prompt_submitted(&mut self, text: &SharedString, cx: &mut Context<Self>) {
        let Some(thread) = self.state.selected_thread_mut() else {
            return;
        };
        thread.messages.push(model::Message {
            role: Role::User,
            text: text.clone(),
        });
        thread.messages.push(model::Message {
            role: Role::Agent,
            text: "Received — the Rust client is not wired to a 67 server yet.".into(),
        });
        cx.notify();
    }

    fn select_thread(&mut self, project: usize, thread: usize, cx: &mut Context<Self>) {
        self.state.selected = Some((project, thread));
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
                for (ti, thread) in project.threads.iter().enumerate() {
                    let active = self.state.selected == Some((pi, ti));
                    section = section.child(
                        div()
                            .id(("thread", pi * 1000 + ti))
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
                                    this.select_thread(pi, ti, cx)
                                }),
                            )
                            .child(thread.title.clone()),
                    );
                }
            }
            projects = projects.child(section);
        }

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
                    .text_color(theme::fg_muted())
                    .child("Local · not connected"),
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

        match self.state.selected_thread() {
            Some(thread) if !thread.messages.is_empty() => {
                for (i, message) in thread.messages.iter().enumerate() {
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
            }
            _ => {
                list = list.child(
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
