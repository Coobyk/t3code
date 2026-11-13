use gpui::{Rgba, rgb, rgba};

/// 67 Code dark palette, mirroring apps/web/src/index.css `:root` dark vars.
pub fn canvas() -> Rgba {
    rgb(0x000000)
}
pub fn sidebar() -> Rgba {
    rgb(0x0a0a0a)
}
pub fn raised() -> Rgba {
    rgb(0x141414)
}
pub fn border() -> Rgba {
    rgb(0x262626)
}
pub fn fg() -> Rgba {
    rgb(0xf5f5f5)
}
pub fn fg_muted() -> Rgba {
    rgb(0x737373)
}
pub fn primary() -> Rgba {
    rgb(0x5a7cff)
}
pub fn hover() -> Rgba {
    rgba(0xffffff0a)
}
pub fn selected() -> Rgba {
    rgba(0xffffff14)
}
pub fn user_bubble() -> Rgba {
    rgb(0x1c1c1e)
}
pub fn cursor() -> Rgba {
    rgb(0x5a7cff)
}
pub fn selection() -> Rgba {
    rgba(0x5a7cff40)
}
