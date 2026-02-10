/// Comparison operators for leaf node filters.
///
/// These operators are used to compare tag values against filter values.
/// All string representations are lowercase for consistency with the protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    /// Equality: tag_value == filter_value
    Equal,
    /// Inequality: tag_value != filter_value
    NotEqual,
    /// Set membership: tag_value in filter_values
    In,
    /// Set non-membership: tag_value not in filter_values
    NotIn,
    /// Key exists in tags
    Exists,
    /// Key does not exist in tags
    NotExists,
    /// String starts with: tag_value.starts_with(filter_value)
    StartsWith,
    /// String ends with: tag_value.ends_with(filter_value)
    EndsWith,
    /// String contains: tag_value.contains(filter_value)
    Contains,
    /// Numeric greater than: tag_value > filter_value
    GreaterThan,
    /// Numeric greater than or equal: tag_value >= filter_value
    GreaterThanOrEqual,
    /// Numeric less than: tag_value < filter_value
    LessThan,
    /// Numeric less than or equal: tag_value <= filter_value
    LessThanOrEqual,
}

impl CompareOp {
    /// Converts a string to a CompareOp.
    ///
    /// Returns None if the string is not a valid operator.
    /// Note: This method is kept for backward compatibility. Prefer using the FromStr trait.
    #[inline]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "eq" => Some(Self::Equal),
            "neq" => Some(Self::NotEqual),
            "in" => Some(Self::In),
            "nin" => Some(Self::NotIn),
            "ex" => Some(Self::Exists),
            "nex" => Some(Self::NotExists),
            "sw" => Some(Self::StartsWith),
            "ew" => Some(Self::EndsWith),
            "ct" => Some(Self::Contains),
            "gt" => Some(Self::GreaterThan),
            "gte" => Some(Self::GreaterThanOrEqual),
            "lt" => Some(Self::LessThan),
            "lte" => Some(Self::LessThanOrEqual),
            _ => None,
        }
    }

    /// Returns the string representation as a static str (zero allocation).
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Equal => "eq",
            Self::NotEqual => "neq",
            Self::In => "in",
            Self::NotIn => "nin",
            Self::Exists => "ex",
            Self::NotExists => "nex",
            Self::StartsWith => "sw",
            Self::EndsWith => "ew",
            Self::Contains => "ct",
            Self::GreaterThan => "gt",
            Self::GreaterThanOrEqual => "gte",
            Self::LessThan => "lt",
            Self::LessThanOrEqual => "lte",
        }
    }
}

// Implement Display trait for CompareOp
impl std::fmt::Display for CompareOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// Implement FromStr trait for CompareOp
impl std::str::FromStr for CompareOp {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s).ok_or_else(|| format!("Invalid comparison operator: {}", s))
    }
}

/// Logical operators for branch nodes in the filter tree.
///
/// These operators combine multiple filter nodes into more complex expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalOp {
    /// Logical AND: all child nodes must match
    And,
    /// Logical OR: at least one child node must match
    Or,
    /// Logical NOT: negates the single child node
    Not,
}

impl LogicalOp {
    /// Converts a string to a LogicalOp.
    ///
    /// Returns None if the string is not a valid operator.
    /// Note: This method is kept for backward compatibility. Prefer using the FromStr trait.
    #[inline]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "and" => Some(Self::And),
            "or" => Some(Self::Or),
            "not" => Some(Self::Not),
            _ => None,
        }
    }

    /// Returns the string representation as a static str (zero allocation).
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::And => "and",
            Self::Or => "or",
            Self::Not => "not",
        }
    }
}

// Implement Display trait for LogicalOp
impl std::fmt::Display for LogicalOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// Implement FromStr trait for LogicalOp
impl std::str::FromStr for LogicalOp {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s).ok_or_else(|| format!("Invalid logical operator: {}", s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_op_from_str() {
        assert_eq!(CompareOp::parse("eq"), Some(CompareOp::Equal));
        assert_eq!(CompareOp::parse("neq"), Some(CompareOp::NotEqual));
        assert_eq!(CompareOp::parse("in"), Some(CompareOp::In));
        assert_eq!(CompareOp::parse("nin"), Some(CompareOp::NotIn));
        assert_eq!(CompareOp::parse("ex"), Some(CompareOp::Exists));
        assert_eq!(CompareOp::parse("nex"), Some(CompareOp::NotExists));
        assert_eq!(CompareOp::parse("sw"), Some(CompareOp::StartsWith));
        assert_eq!(CompareOp::parse("ew"), Some(CompareOp::EndsWith));
        assert_eq!(CompareOp::parse("ct"), Some(CompareOp::Contains));
        assert_eq!(CompareOp::parse("gt"), Some(CompareOp::GreaterThan));
        assert_eq!(CompareOp::parse("gte"), Some(CompareOp::GreaterThanOrEqual));
        assert_eq!(CompareOp::parse("lt"), Some(CompareOp::LessThan));
        assert_eq!(CompareOp::parse("lte"), Some(CompareOp::LessThanOrEqual));
        assert_eq!(CompareOp::parse("invalid"), None);
    }

    #[test]
    fn test_compare_op_as_str() {
        assert_eq!(CompareOp::Equal.as_str(), "eq");
        assert_eq!(CompareOp::NotEqual.as_str(), "neq");
        assert_eq!(CompareOp::In.as_str(), "in");
        assert_eq!(CompareOp::NotIn.as_str(), "nin");
        assert_eq!(CompareOp::Exists.as_str(), "ex");
        assert_eq!(CompareOp::NotExists.as_str(), "nex");
        assert_eq!(CompareOp::StartsWith.as_str(), "sw");
        assert_eq!(CompareOp::EndsWith.as_str(), "ew");
        assert_eq!(CompareOp::Contains.as_str(), "ct");
        assert_eq!(CompareOp::GreaterThan.as_str(), "gt");
        assert_eq!(CompareOp::GreaterThanOrEqual.as_str(), "gte");
        assert_eq!(CompareOp::LessThan.as_str(), "lt");
        assert_eq!(CompareOp::LessThanOrEqual.as_str(), "lte");
    }

    #[test]
    fn test_logical_op_from_str() {
        assert_eq!(LogicalOp::parse("and"), Some(LogicalOp::And));
        assert_eq!(LogicalOp::parse("or"), Some(LogicalOp::Or));
        assert_eq!(LogicalOp::parse("not"), Some(LogicalOp::Not));
        assert_eq!(LogicalOp::parse("invalid"), None);
    }

    #[test]
    fn test_logical_op_as_str() {
        assert_eq!(LogicalOp::And.as_str(), "and");
        assert_eq!(LogicalOp::Or.as_str(), "or");
        assert_eq!(LogicalOp::Not.as_str(), "not");
    }

    #[test]
    fn test_round_trip_compare_ops() {
        let ops = [
            CompareOp::Equal,
            CompareOp::NotEqual,
            CompareOp::In,
            CompareOp::NotIn,
            CompareOp::Exists,
            CompareOp::NotExists,
            CompareOp::StartsWith,
            CompareOp::EndsWith,
            CompareOp::Contains,
            CompareOp::GreaterThan,
            CompareOp::GreaterThanOrEqual,
            CompareOp::LessThan,
            CompareOp::LessThanOrEqual,
        ];

        for op in ops {
            let s = op.as_str();
            let parsed = CompareOp::parse(s);
            assert_eq!(parsed, Some(op));
        }
    }

    #[test]
    fn test_round_trip_logical_ops() {
        let ops = [LogicalOp::And, LogicalOp::Or, LogicalOp::Not];

        for op in ops {
            let s = op.as_str();
            let parsed = LogicalOp::parse(s);
            assert_eq!(parsed, Some(op));
        }
    }
}
