//! Theme file for Terminal UI rendering

use std::ops::Deref;
use std::path::Path;
use std::str::FromStr;

use anyhow::Context as _;
use ratatui::{
    style::{Color, Modifier, Style},
    widgets::BorderType,
};
use serde::{Deserialize, de::Error};

/// App theme
#[derive(Deserialize, Default, Debug, PartialEq)]
#[serde(default)]
pub struct AppTheme {
    pub main: AppMainTheme,
    pub help: AppHelpTheme,
    pub confirm_exit: AppConfirmExitTheme,
}

impl AppTheme {
    pub fn load(path: Option<&Path>) -> anyhow::Result<Self> {
        let theme_str = if let Some(path) = path {
            std::fs::read_to_string(path)
                .with_context(|| format!("failed to read theme file at '{}'", path.display()))?
        } else {
            String::from(include_str!("../../etc/theme.yaml"))
        };
        Self::from_str(&theme_str)
    }
}
impl FromStr for AppTheme {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(serde_yaml::from_str::<Self>(s)?)
    }
}

/// Them for Main screen
#[derive(Deserialize, Default, Debug, PartialEq)]
#[serde(default)]
pub struct AppMainTheme {
    pub bars: ThemeStyle,
    pub title: ThemeStyle,
    /// Style for key strokes
    pub key_stroke: ThemeStyle,
    /// Style for active view (F5/F6/F7)
    pub active_view: ThemeStyle,
}

/// Theme for Help screen
#[derive(Deserialize, Default, Debug, PartialEq)]
#[serde(default)]
pub struct AppHelpTheme {
    pub border_style: ThemeStyle,
    pub border_type: ThemeBorderType,
    pub content: ThemeStyle,
    /// Style for key strokes
    pub content_key_stroke: ThemeStyle,
    /// Style for highlighted content (app name)
    pub content_highlight: ThemeStyle,
}

/// Theme for `ConfirmExit` dialog box
#[derive(Deserialize, Default, Debug, PartialEq)]
#[serde(default)]
pub struct AppConfirmExitTheme {
    pub border_style: ThemeStyle,
    pub border_type: ThemeBorderType,
    pub content: ThemeStyle,
}

/// Style
#[derive(Deserialize, Default, Debug, PartialEq)]
pub struct ThemeStyle {
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
            instance = instance.fg(**fg);
        }
        if let Some(bg) = &style.bg {
            instance = instance.bg(**bg);
        }
        if let Some(effect) = &style.effect {
            instance = instance.add_modifier(**effect);
        }
        instance
    }
}

/// Color
#[derive(Debug, PartialEq)]
pub struct ThemeColor(Color);

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

impl Deref for ThemeColor {
    type Target = Color;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Text effect
#[derive(Debug, PartialEq)]
pub struct TextEffect(Modifier);

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

impl Deref for TextEffect {
    type Target = Modifier;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Border type
#[derive(Default, Debug, PartialEq)]
pub struct ThemeBorderType(BorderType);

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

impl Deref for ThemeBorderType {
    type Target = BorderType;

    fn deref(&self) -> &Self::Target {
        &self.0
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
