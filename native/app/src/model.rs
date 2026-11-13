use gpui::SharedString;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Role {
    User,
    Agent,
}

pub struct Message {
    pub role: Role,
    pub text: SharedString,
}

pub struct Thread {
    pub title: SharedString,
    pub messages: Vec<Message>,
}

pub struct Project {
    pub name: SharedString,
    pub path: SharedString,
    pub expanded: bool,
    pub threads: Vec<Thread>,
}

pub struct AppState {
    pub projects: Vec<Project>,
    pub selected: Option<(usize, usize)>,
}

impl AppState {
    pub fn mock() -> Self {
        Self {
            projects: vec![
                Project {
                    name: "67code".into(),
                    path: "~/Projects/67code".into(),
                    expanded: true,
                    threads: vec![
                        Thread {
                            title: "Rewrite the app in Rust + GPUI".into(),
                            messages: vec![
                                Message {
                                    role: Role::User,
                                    text: "Rewrite the desktop app in Rust and GPUI.".into(),
                                },
                                Message {
                                    role: Role::Agent,
                                    text: "On it. Scaffolding a GPUI shell on the dev box, committing as I go.".into(),
                                },
                                Message {
                                    role: Role::User,
                                    text: "Make sure it renders on the box desktop.".into(),
                                },
                                Message {
                                    role: Role::Agent,
                                    text: "Verified: window opens on display :1 with the 67 Code shell.".into(),
                                },
                            ],
                        },
                        Thread {
                            title: "Fix Electron scheme crash".into(),
                            messages: vec![],
                        },
                        Thread {
                            title: "Rebrand sweep".into(),
                            messages: vec![],
                        },
                    ],
                },
                Project {
                    name: "web-playground".into(),
                    path: "~/Projects/web-playground".into(),
                    expanded: true,
                    threads: vec![Thread {
                        title: "Sandbox test".into(),
                        messages: vec![],
                    }],
                },
            ],
            selected: Some((0, 0)),
        }
    }

    pub fn selected_thread(&self) -> Option<&Thread> {
        let (p, t) = self.selected?;
        self.projects.get(p)?.threads.get(t)
    }

    pub fn selected_thread_mut(&mut self) -> Option<&mut Thread> {
        let (p, t) = self.selected?;
        self.projects.get_mut(p)?.threads.get_mut(t)
    }
}
