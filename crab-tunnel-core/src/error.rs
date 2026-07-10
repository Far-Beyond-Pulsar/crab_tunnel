use thiserror::Error;

#[derive(Debug, Error)]
pub enum HolePunchError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    #[error("Server error: {0}")]
    Server(String),

    #[error("Peer '{0}' not found on server")]
    PeerNotFound(String),

    #[error("Timed out waiting for response")]
    Timeout,

    #[error("No response from server")]
    NoResponse,

    #[error("Hole punch failed — could not establish direct connection")]
    PunchFailed,

    #[error("Socket error: {0}")]
    Socket(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        assert_eq!(
            HolePunchError::PeerNotFound("alice".into()).to_string(),
            "Peer 'alice' not found on server"
        );
        assert_eq!(
            HolePunchError::PunchFailed.to_string(),
            "Hole punch failed — could not establish direct connection"
        );
        assert_eq!(
            HolePunchError::Timeout.to_string(),
            "Timed out waiting for response"
        );
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused");
        let hp_err: HolePunchError = io_err.into();
        assert!(matches!(hp_err, HolePunchError::Io(_)));
    }

    #[test]
    fn test_error_from_bincode() {
        let bincode_err = bincode::Error::from(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid data",
        ));
        let hp_err: HolePunchError = bincode_err.into();
        assert!(matches!(hp_err, HolePunchError::Serialization(_)));
    }
}
