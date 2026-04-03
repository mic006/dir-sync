//! Theme file for Terminal UI rendering

use std::path::Path;
use std::str::FromStr;

use anyhow::Context as _;
use ratatui::{
    style::{Color, Modifier, Style},
    widgets::BorderType,
};
use serde::{Deserialize, de::Error};

/// App theme
#[derive(Deserialize, Debug, PartialEq)]
pub struct AppTheme {
    /// Display height repartition between list of diff and content of one diff
    /// Ex: `fill_factor_list=1`, `fill_factor_content=3` allocates 1/4 for list and 3/4 for content
    fill_factor_list: u16,
    fill_factor_content: u16,
    /// Default style for all content
    content: ThemeStyle,
    bar: AppBarTheme,
    diff_list: AppDiffListTheme,
    diff_content: AppDiffContentTheme,
    help: AppHelpTheme,
    confirm_exit: AppConfirmExitTheme,
}

impl AppTheme {
    /// Load theme from file
    ///
    /// # Errors
    /// - invalid path
    /// - invalid theme file
    pub fn load(path: Option<&Path>) -> anyhow::Result<Self> {
        let theme_str = if let Some(path) = path {
            std::fs::read_to_string(path)
                .with_context(|| format!("failed to read theme file at '{}'", path.display()))?
        } else {
            String::from(include_str!("../../etc/theme.yaml"))
        };
        Self::from_str(&theme_str)
    }

    /// Get fill factors for list vs content (up pane vs bottom pane)
    #[must_use]
    pub fn fill_factors(&self) -> (u16, u16) {
        (self.fill_factor_list, self.fill_factor_content)
    }

    /// Get content style
    #[must_use]
    pub fn content_style(&self) -> Style {
        Style::from(&self.content)
    }

    /// Get bars, headers, separators style
    #[must_use]
    pub fn bar_style(&self) -> Style {
        self.content_style().patch(Style::from(&self.bar.base))
    }

    /// Get main title style
    #[must_use]
    pub fn bar_title_style(&self) -> Style {
        self.bar_style().patch(Style::from(&self.bar.title))
    }

    /// Get keystroke style patch
    #[must_use]
    pub fn bar_key_stroke_style_patch(&self) -> Style {
        Style::from(&self.bar.key_stroke)
    }

    /// Get active view style
    #[must_use]
    pub fn bar_active_view_style(&self) -> Style {
        self.bar_style().patch(Style::from(&self.bar.active_view))
    }

    /// Get scroll bar style in main panes
    #[must_use]
    pub fn bar_scroll_bar_style(&self) -> Style {
        self.content_style()
            .patch(Style::from(&self.bar.scroll_bar))
    }

    /// Get selected item style in main panes
    #[must_use]
    pub fn diff_list_selected_item_style(&self) -> Style {
        self.content_style()
            .patch(Style::from(&self.diff_list.selected_item))
    }

    /// Get highlight sync action in diff list style patch
    #[must_use]
    pub fn diff_list_sync_resolved_style_patch(&self) -> Style {
        Style::from(&self.diff_list.sync_resolved)
    }

    /// Get highlight sync conflict in diff list style patch
    #[must_use]
    pub fn diff_list_sync_conflict_style_patch(&self) -> Style {
        Style::from(&self.diff_list.sync_conflict)
    }

    /// Get old content base style
    #[must_use]
    pub fn diff_content_old_base_style(&self) -> Style {
        self.content_style().bg(self.diff_content.old_bg.0)
    }

    /// Get new content base style
    #[must_use]
    pub fn diff_content_new_base_style(&self) -> Style {
        self.content_style().bg(self.diff_content.new_bg.0)
    }

    /// Get conflict content base style
    #[must_use]
    pub fn diff_content_conflict_base_style(&self) -> Style {
        self.content_style().bg(self.diff_content.conflict_bg.0)
    }

    /// Get old content highlight style patch
    #[must_use]
    pub fn diff_content_old_highlight_style_patch(&self) -> Style {
        Style::new().fg(self.diff_content.old_fg.0)
    }

    /// Get new content highlight style patch
    #[must_use]
    pub fn diff_content_new_highlight_style_patch(&self) -> Style {
        Style::new().fg(self.diff_content.new_fg.0)
    }

    /// Get conflict content highlight style patch
    #[must_use]
    pub fn diff_content_conflict_highlight_style_patch(&self) -> Style {
        Style::new().fg(self.diff_content.conflict_fg.0)
    }

    /// Get line number style
    #[must_use]
    pub fn diff_content_line_num_style(&self) -> Style {
        self.content_style()
            .patch(Style::from(&self.diff_content.line_num))
    }

    /// Get border style for help screen
    #[must_use]
    pub fn help_border_style(&self) -> Style {
        self.content_style()
            .patch(Style::from(&self.help.border_style))
    }

    /// Get border type for help screen
    #[must_use]
    pub fn help_border_type(&self) -> BorderType {
        self.help.border_type.0
    }

    /// Get key stroke style for help screen
    #[must_use]
    pub fn help_key_stroke_style(&self) -> Style {
        self.content_style()
            .patch(Style::from(&self.help.key_stroke))
    }

    /// Get highlight style for help screen
    #[must_use]
    pub fn help_highlight_style(&self) -> Style {
        self.content_style()
            .patch(Style::from(&self.help.highlight))
    }

    /// Get border style for confirm exit screen
    #[must_use]
    pub fn confirm_exit_border_style(&self) -> Style {
        self.content_style()
            .patch(Style::from(&self.confirm_exit.border_style))
    }

    /// Get border type for confirm exit screen
    #[must_use]
    pub fn confirm_exit_border_type(&self) -> BorderType {
        self.confirm_exit.border_type.0
    }
}
impl FromStr for AppTheme {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(serde_yaml::from_str::<Self>(s)?)
    }
}

/// Theme for Bars, headers...
#[derive(Deserialize, Default, Debug, PartialEq)]
#[serde(default)]
struct AppBarTheme {
    /// Base style for bars, headers, separators
    base: ThemeStyle,
    /// Main title style
    title: ThemeStyle,
    /// Style for key strokes
    key_stroke: ThemeStyle,
    /// Style for active view (F5/F6/F7)
    active_view: ThemeStyle,
    scroll_bar: ThemeStyle,
}

/// Theme for Main screen, Diff list pane
#[derive(Deserialize, Default, Debug, PartialEq)]
#[serde(default)]
struct AppDiffListTheme {
    selected_item: ThemeStyle,
    /// Highlight sync action in diff list
    sync_resolved: ThemeStyle,
    /// Highlight sync conflict in diff list
    sync_conflict: ThemeStyle,
}

/// Theme for Main screen, Diff content pane
#[derive(Deserialize, Debug, PartialEq)]
struct AppDiffContentTheme {
    old_fg: ThemeColor,
    old_bg: ThemeColor,
    new_fg: ThemeColor,
    new_bg: ThemeColor,
    conflict_fg: ThemeColor,
    conflict_bg: ThemeColor,
    line_num: ThemeStyle,
}

/// Theme for Help screen
#[derive(Deserialize, Default, Debug, PartialEq)]
#[serde(default)]
struct AppHelpTheme {
    border_style: ThemeStyle,
    border_type: ThemeBorderType,
    /// Style for key strokes
    key_stroke: ThemeStyle,
    /// Style for highlighted content (app name)
    highlight: ThemeStyle,
}

/// Theme for `ConfirmExit` dialog box
#[derive(Deserialize, Default, Debug, PartialEq)]
#[serde(default)]
struct AppConfirmExitTheme {
    border_style: ThemeStyle,
    border_type: ThemeBorderType,
}

/// Style
#[derive(Deserialize, Default, Debug, PartialEq)]
struct ThemeStyle {
    /// Foreground color
    fg: Option<ThemeColor>,
    /// Background color
    bg: Option<ThemeColor>,
    /// Text effect
    effect: Option<TextEffect>,
}

impl From<&ThemeStyle> for Style {
    fn from(style: &ThemeStyle) -> Self {
        let mut instance = Style::default();
        if let Some(fg) = &style.fg {
            instance = instance.fg(fg.0);
        }
        if let Some(bg) = &style.bg {
            instance = instance.bg(bg.0);
        }
        if let Some(effect) = &style.effect {
            instance = instance.add_modifier(effect.0);
        }
        instance
    }
}

/// Color
#[derive(Debug, PartialEq)]
struct ThemeColor(Color);

impl<'de> Deserialize<'de> for ThemeColor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::from_str(&s).map_err(D::Error::custom)
    }
}

impl FromStr for ThemeColor {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() == 7 && s.starts_with('#') {
            let rgb = u32::from_str_radix(&s[1..], 16)
                .map_err(|_| anyhow::anyhow!("invalid RGB color, not hexadecimal value"))?;
            Ok(Self(Color::from_u32(rgb)))
        } else {
            let color_code = u8::from_str(s).map_err(|_| {
                anyhow::anyhow!(
                    "invalid color, expecting a color code (0-255) or a RGB value (#0088FF)"
                )
            })?;
            Ok(Self(Color::Indexed(color_code)))
        }
    }
}

/// Text effect
#[derive(Debug, PartialEq)]
struct TextEffect(Modifier);

impl<'de> Deserialize<'de> for TextEffect {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::from_str(&s).map_err(D::Error::custom)
    }
}

impl FromStr for TextEffect {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut modifier = Modifier::empty();

        for field in s.split(['+', '|', ',']) {
            let field = field.trim();
            match field {
                "" => (),
                "bold" => modifier |= Modifier::BOLD,
                "dim" => modifier |= Modifier::DIM,
                "italic" => modifier |= Modifier::ITALIC,
                "underlined" => modifier |= Modifier::UNDERLINED,
                "reversed" => modifier |= Modifier::REVERSED,
                _ => {
                    anyhow::bail!("unknown text effect '{field}'");
                }
            }
        }

        Ok(Self(modifier))
    }
}

/// Border type
#[derive(Default, Debug, PartialEq)]
struct ThemeBorderType(BorderType);

impl<'de> Deserialize<'de> for ThemeBorderType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::from_str(&s).map_err(D::Error::custom)
    }
}

impl FromStr for ThemeBorderType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "plain" => Ok(Self(BorderType::Plain)),
            "rounded" => Ok(Self(BorderType::Rounded)),
            "double" => Ok(Self(BorderType::Double)),
            "thick" => Ok(Self(BorderType::Thick)),
            "light-double-dashed" => Ok(Self(BorderType::LightDoubleDashed)),
            "heavy-double-dashed" => Ok(Self(BorderType::HeavyDoubleDashed)),
            "light-triple-dashed" => Ok(Self(BorderType::LightTripleDashed)),
            "heavy-triple-dashed" => Ok(Self(BorderType::HeavyTripleDashed)),
            "light-quadruple-dashed" => Ok(Self(BorderType::LightQuadrupleDashed)),
            "heavy-quadruple-dashed" => Ok(Self(BorderType::HeavyQuadrupleDashed)),
            "quadrant-inside" => Ok(Self(BorderType::QuadrantInside)),
            "quadrant-outside" => Ok(Self(BorderType::QuadrantOutside)),
            _ => anyhow::bail!("unknown border type '{s}'"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ok() {
        for (input, expected) in [
            ("#000000", Color::Rgb(0, 0, 0)),
            ("#123456", Color::Rgb(0x12, 0x34, 0x56)),
            ("#ffffff", Color::Rgb(255, 255, 255)),
            ("#FFFFFF", Color::Rgb(255, 255, 255)),
            ("0", Color::Indexed(0)),
            ("255", Color::Indexed(255)),
        ] {
            assert_eq!(ThemeColor::from_str(input).unwrap(), ThemeColor(expected));
        }
    }

    #[test]
    fn parse_err() {
        for input in [
            "#",        // too short
            "#12345",   // too short
            "#1234567", // too long
            "#GGGGGG",  // invalid hex
            "256",      // overflow
            "-1",       // invalid int
            "abc",      // invalid
        ] {
            assert!(
                ThemeColor::from_str(input).is_err(),
                "Should fail for {input}"
            );
        }
    }

    #[test]
    fn parse_effect_ok() {
        for (input, expected) in [
            ("", Modifier::empty()),
            ("bold", Modifier::BOLD),
            ("dim", Modifier::DIM),
            ("italic", Modifier::ITALIC),
            ("underlined", Modifier::UNDERLINED),
            ("reversed", Modifier::REVERSED),
            ("bold+italic", Modifier::BOLD | Modifier::ITALIC),
            ("dim|reversed", Modifier::DIM | Modifier::REVERSED),
            ("underlined, bold", Modifier::BOLD | Modifier::UNDERLINED),
            (" bold + underlined ", Modifier::BOLD | Modifier::UNDERLINED),
        ] {
            assert_eq!(
                TextEffect::from_str(input).unwrap(),
                TextEffect(expected),
                "Failed for {input}"
            );
        }
    }

    #[test]
    fn parse_effect_err() {
        for input in ["unknown", "bold+unknown", "bold & italic"] {
            assert!(
                TextEffect::from_str(input).is_err(),
                "Should fail for {input}"
            );
        }
    }

    #[test]
    fn parse_border_type_ok() {
        for (input, expected) in [
            ("plain", BorderType::Plain),
            ("rounded", BorderType::Rounded),
            ("double", BorderType::Double),
            ("thick", BorderType::Thick),
            ("light-double-dashed", BorderType::LightDoubleDashed),
            ("heavy-double-dashed", BorderType::HeavyDoubleDashed),
            ("light-triple-dashed", BorderType::LightTripleDashed),
            ("heavy-triple-dashed", BorderType::HeavyTripleDashed),
            ("light-quadruple-dashed", BorderType::LightQuadrupleDashed),
            ("heavy-quadruple-dashed", BorderType::HeavyQuadrupleDashed),
            ("quadrant-inside", BorderType::QuadrantInside),
            ("quadrant-outside", BorderType::QuadrantOutside),
        ] {
            assert_eq!(
                ThemeBorderType::from_str(input).unwrap(),
                ThemeBorderType(expected),
                "Failed for {input}"
            );
        }
    }

    #[test]
    fn parse_border_type_err() {
        for input in ["", "unknown", "Plain"] {
            assert!(
                ThemeBorderType::from_str(input).is_err(),
                "Should fail for {input}"
            );
        }
    }
}
