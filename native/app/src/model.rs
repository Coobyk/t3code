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

#[derive(Debug, Clone)]
pub struct Project {
    pub id: SharedString,
    pub name: SharedString,
    pub path: SharedString,
    pub expanded: bool,
}

#[derive(Debug, Clone)]
pub struct Thread {
    pub id: SharedString,
    pub project_id: SharedString,
    pub title: SharedString,
    pub status: SharedString,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionStatus {
    Mock,
    Connecting,
    Connected,
    Failed(SharedString),
    Disconnected(SharedString),
}

pub struct AppState {
    pub projects: Vec<Project>,
    pub threads: Vec<Thread>,
    /// Selected thread id.
    pub selected: Option<SharedString>,
    /// Mock messages shown per thread while the client is offline or before
    /// `orchestration.subscribeThread` lands.
    pub messages: Vec<Message>,
    pub connection: ConnectionStatus,
}

impl AppState {
    pub fn mock() -> Self {
        Self {
            projects: vec![
                Project {
                    id: "p-67code".into(),
                    name: "67code".into(),
                    path: "~/Projects/67code".into(),
                    expanded: true,
                },
                Project {
                    id: "p-web".into(),
                    name: "web-playground".into(),
                    path: "~/Projects/web-playground".into(),
                    expanded: true,
                },
            ],
            threads: vec![
                Thread {
                    id: "t-rust".into(),
                    project_id: "p-67code".into(),
                    title: "Rewrite the app in Rust + GPUI".into(),
                    status: "idle".into(),
                },
                Thread {
                    id: "t-scheme".into(),
                    project_id: "p-67code".into(),
                    title: "Fix Electron scheme crash".into(),
                    status: "idle".into(),
                },
                Thread {
                    id: "t-rebrand".into(),
                    project_id: "p-67code".into(),
                    title: "Rebrand sweep".into(),
                    status: "idle".into(),
                },
                Thread {
                    id: "t-sandbox".into(),
                    project_id: "p-web".into(),
                    title: "Sandbox test".into(),
                    status: "idle".into(),
                },
            ],
            selected: Some("t-rust".into()),
            messages: vec![
                Message {
                    role: Role::User,
                    text: "Rewrite the desktop app in Rust and GPUI.".into(),
                },
                Message {
                    role: Role::Agent,
                    text: "On it. Scaffolding a GPUI shell on the dev box, committing as I go."
                        .into(),
                },
            ],
            connection: ConnectionStatus::Mock,
        }
    }

    pub fn connecting() -> Self {
        Self {
            projects: Vec::new(),
            threads: Vec::new(),
            selected: None,
            messages: Vec::new(),
            connection: ConnectionStatus::Connecting,
        }
    }

    pub fn threads_for<'a>(&'a self, project_id: &'a str) -> impl Iterator<Item = &'a Thread> {
        self.threads
            .iter()
            .filter(move |t| t.project_id.as_ref() == project_id)
    }

    pub fn selected_thread(&self) -> Option<&Thread> {
        let id = self.selected.as_ref()?;
        self.threads.iter().find(|t| &t.id == id)
    }
}
