#[cfg(test)]
mod tests {
    use crossfire::mpsc;
    use sockudo_adapter::cleanup::{CleanupSender, DisconnectTask};
    use sockudo_core::websocket::SocketId;
    use std::time::Instant;

    fn create_test_task(socket_id: &str) -> DisconnectTask {
        DisconnectTask {
            socket_id: SocketId::from_string(socket_id).unwrap_or_default(),
            app_id: "test-app".to_string(),
            subscribed_channels: vec!["channel1".to_string(), "channel2".to_string()],
            user_id: Some("user123".to_string()),
            timestamp: Instant::now(),
            connection_info: None,
        }
    }

    #[tokio::test]
    async fn test_cleanup_sender_direct_variant() {
        let (tx, rx) = mpsc::bounded_async::<DisconnectTask>(10);
        let sender = CleanupSender::Direct(tx);

        let task = create_test_task("socket1");
        let task_clone = task.clone();

        assert!(sender.try_send(task).is_ok());

        let received = rx.recv().await.unwrap();
        assert_eq!(
            received.socket_id.to_string(),
            task_clone.socket_id.to_string()
        );
        assert_eq!(received.app_id, task_clone.app_id);
    }

    #[tokio::test]
    async fn test_cleanup_sender_direct_backpressure() {
        let (tx, _rx) = mpsc::bounded_async::<DisconnectTask>(2);
        let sender = CleanupSender::Direct(tx);

        assert!(sender.try_send(create_test_task("socket1")).is_ok());
        assert!(sender.try_send(create_test_task("socket2")).is_ok());

        let result = sender.try_send(create_test_task("socket3"));
        assert!(result.is_err());

        let err = result.unwrap_err();

        match err.as_ref() {
            crossfire::TrySendError::Full(_) => {}
            _ => panic!("Expected Full error"),
        }
    }

    #[tokio::test]
    async fn test_cleanup_sender_try_send_interface() {
        let (tx, rx) = mpsc::bounded_async::<DisconnectTask>(10);
        let sender = CleanupSender::Direct(tx);

        let task = create_test_task("socket1");
        let task_clone = task.clone();

        match sender.try_send(task) {
            Ok(()) => {
                let received = rx.recv().await.unwrap();
                assert_eq!(
                    received.socket_id.to_string(),
                    task_clone.socket_id.to_string()
                );
            }
            Err(_) => panic!("Expected successful send"),
        }
    }

    #[tokio::test]
    async fn test_cleanup_sender_is_closed() {
        let (tx, rx) = mpsc::bounded_async::<DisconnectTask>(10);
        let sender = CleanupSender::Direct(tx);

        assert!(!sender.is_closed());

        drop(rx);

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        assert!(sender.is_closed());
    }

    #[tokio::test]
    async fn test_cleanup_sender_clone_semantics() {
        let (tx, rx) = mpsc::bounded_async::<DisconnectTask>(10);
        let sender = CleanupSender::Direct(tx);
        let sender_clone = sender.clone();

        assert!(sender.try_send(create_test_task("socket1")).is_ok());
        assert!(sender_clone.try_send(create_test_task("socket2")).is_ok());

        let _ = rx.recv().await.unwrap();
        let _ = rx.recv().await.unwrap();
    }

    #[tokio::test]
    async fn test_cleanup_sender_error_types() {
        let (tx, rx) = mpsc::bounded_async::<DisconnectTask>(1);
        let sender = CleanupSender::Direct(tx);

        assert!(sender.try_send(create_test_task("socket1")).is_ok());

        let task2 = create_test_task("socket2");
        let result = sender.try_send(task2.clone());
        assert!(result.is_err());

        let err = result.unwrap_err();
        match err.as_ref() {
            crossfire::TrySendError::Full(returned_task) => {
                assert_eq!(
                    returned_task.socket_id.to_string(),
                    task2.socket_id.to_string()
                );
                assert_eq!(returned_task.app_id, task2.app_id);
            }
            other => panic!("Expected Full error, got: {:?}", other),
        }

        drop(rx);
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let task3 = create_test_task("socket3");
        let result = sender.try_send(task3.clone());
        assert!(result.is_err());

        let err = result.unwrap_err();
        match err.as_ref() {
            crossfire::TrySendError::Disconnected(returned_task) => {
                assert_eq!(
                    returned_task.socket_id.to_string(),
                    task3.socket_id.to_string()
                );
                assert_eq!(returned_task.app_id, task3.app_id);
            }
            other => panic!("Expected Closed error, got: {:?}", other),
        }
    }
}
