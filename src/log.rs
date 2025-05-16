// src/logger.rs
use chrono::Local;
use colored::*;
use serde_json::Value;
use std::fmt::Display;

pub struct Log {
    pub debug: bool,
}

impl Log {
    pub fn info_title<T: Display>(message: T) {
        Self::log(message, &["bold", "black", "on_cyan"], 2, 1);
    }

    pub fn success_title<T: Display>(message: T) {
        Self::log(message, &["bold", "black", "on_green"], 2, 1);
    }

    pub fn error_title<T: Display>(message: T) {
        Self::log(
            Self::prefix_with_time(message),
            &["bold", "black", "on_red"],
            2,
            1,
        );
    }

    pub fn warning_title<T: Display>(message: T) {
        Self::log(
            Self::prefix_with_time(message),
            &["bold", "black", "on_yellow"],
            2,
            1,
        );
    }

    pub fn cluster_title<T: Display>(message: T) {
        Self::log(
            Self::prefix_with_time(message),
            &["bold", "yellow", "on_magenta"],
            2,
            1,
        );
    }

    pub fn http_title<T: Display>(message: T) {
        Self::info_title(Self::prefix_with_time(message));
    }

    pub fn discover_title<T: Display>(message: T) {
        Self::log(
            Self::prefix_with_time(message),
            &["bold", "grey", "on_bright_cyan"],
            2,
            1,
        );
    }

    pub fn websocket_title<T: Display>(message: T) {
        Self::success_title(Self::prefix_with_time(message));
    }

    pub fn webhook_sender_title<T: Display>(message: T) {
        Self::log(
            Self::prefix_with_time(message),
            &["bold", "blue", "on_white"],
            2,
            1,
        );
    }

    pub fn info<T: Display>(message: T) {
        Self::log(message, &["cyan"], 2, 0);
    }

    pub fn success<T: Display>(message: T) {
        Self::log(message, &["green"], 2, 0);
    }

    pub fn error<T: Display>(message: T) {
        Self::log(message, &["red"], 2, 0);
    }

    pub fn warning<T: Display>(message: T) {
        Self::log(message, &["yellow"], 2, 0);
    }

    pub fn cluster<T: Display>(message: T) {
        Self::log(message, &["bold", "magenta"], 2, 0);
    }

    pub fn http<T: Display>(message: T) {
        Self::info(message);
    }

    pub fn discover<T: Display>(message: T) {
        Self::log(message, &["bold", "bright_cyan"], 2, 0);
    }

    pub fn websocket<T: Display>(message: T) {
        Self::success(message);
    }

    pub fn webhook_sender<T: Display>(message: T) {
        Self::log(message, &["bold", "white"], 2, 0);
    }

    pub fn br() {
        println!();
    }

    fn prefix_with_time<T: Display>(message: T) -> String {
        format!("[{}] {}", Local::now().format("%Y-%m-%d %H:%M:%S"), message)
    }

    fn log<T: Display>(message: T, styles: &[&str], margin: usize, padding: usize) {
        let mut colored_message = message.to_string();

        // Apply colors and styles
        for style in styles {
            colored_message = match *style {
                "bold" => colored_message.bold(),
                "black" => colored_message.black(),
                "white" => colored_message.white(),
                "red" => colored_message.red(),
                "green" => colored_message.green(),
                "yellow" => colored_message.yellow(),
                "blue" => colored_message.blue(),
                "magenta" => colored_message.magenta(),
                "cyan" => colored_message.cyan(),
                "grey" | "gray" => colored_message.bright_black(),
                "bright_cyan" => colored_message.bright_cyan(),
                "on_red" => colored_message.on_red(),
                "on_green" => colored_message.on_green(),
                "on_yellow" => colored_message.on_yellow(),
                "on_blue" => colored_message.on_blue(),
                "on_magenta" => colored_message.on_magenta(),
                "on_cyan" => colored_message.on_cyan(),
                "on_white" => colored_message.on_white(),
                _ => colored_message.white(),
            }
            .to_string();
        }

        // Apply margins and padding
        let margin_spaces = " ".repeat(margin);
        let padding_spaces = " ".repeat(padding);

        // Handle JSON values - both string representations and Value types
        let formatted_message =
            if let Ok(json_value) = serde_json::from_str::<Value>(&colored_message) {
                serde_json::to_string_pretty(&json_value).unwrap_or(colored_message)
            } else if let Some(stripped) = colored_message.strip_prefix("Value(") {
                if let Some(stripped) = stripped.strip_suffix(")") {
                    if let Ok(json_value) = serde_json::from_str::<Value>(stripped) {
                        serde_json::to_string_pretty(&json_value).unwrap_or(colored_message)
                    } else {
                        colored_message
                    }
                } else {
                    colored_message
                }
            } else {
                colored_message
            };

        println!(
            "{}{}{}{}",
            margin_spaces, padding_spaces, formatted_message, padding_spaces
        );
    }
}

// Example macro for easier logging with format strings
#[macro_export]
macro_rules! log_info {
    ($($arg:tt)*) => ({
        Log::info(format!($($arg)*));
    })
}

#[macro_export]
macro_rules! log_error {
    ($($arg:tt)*) => ({
        Log::error(format!($($arg)*));
    })
}

// Add to Cargo.toml:
// [dependencies]
// colored = "2.0"
// chrono = "0.4"
// serde_json = "1.0"
