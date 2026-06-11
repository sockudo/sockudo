use super::*;

#[async_trait]
impl<T: HorizontalTransport> HorizontalAdapterInterface for HorizontalAdapterBase<T> {
    /// Broadcast presence member joined to all nodes
    async fn broadcast_presence_join(
        &self,
        app_id: &str,
        channel: &str,
        user_id: &str,
        socket_id: &str,
        user_info: Option<sonic_rs::Value>,
    ) -> Result<()> {
        // Store in our own registry first with a single lock acquisition
        self.horizontal
            .add_presence_entry(
                &self.node_id,
                channel,
                socket_id,
                user_id,
                app_id,
                user_info.clone(),
            )
            .await;

        // Skip cluster broadcast if cluster health is disabled
        if !self.cluster_health_enabled {
            return Ok(());
        }

        let request = RequestBody {
            request_id: crate::horizontal_adapter::generate_request_id(),
            node_id: self.node_id.clone(),
            app_id: app_id.to_string(),
            request_type: RequestType::PresenceMemberJoined,
            channel: Some(channel.to_string()),
            socket_id: Some(socket_id.to_string()),
            user_id: Some(user_id.to_string()),
            // Cluster presence fields
            user_info,
            timestamp: None,
            dead_node_id: None,
            target_node_id: None,
            reply_to: None,
            channels: None,
        };

        // Send without waiting for response (broadcast) - skip if single node
        if !self.should_skip_horizontal_communication().await {
            self.transport.publish_request(&request).await
        } else {
            Ok(())
        }
    }

    /// Broadcast presence member left to all nodes
    async fn broadcast_presence_leave(
        &self,
        app_id: &str,
        channel: &str,
        user_id: &str,
        socket_id: &str,
    ) -> Result<()> {
        // Remove from our own registry first with a single lock acquisition
        self.horizontal
            .remove_presence_entry(&self.node_id, channel, socket_id)
            .await;

        // Skip cluster broadcast if cluster health is disabled
        if !self.cluster_health_enabled {
            return Ok(());
        }

        let request = RequestBody {
            request_id: crate::horizontal_adapter::generate_request_id(),
            node_id: self.node_id.clone(),
            app_id: app_id.to_string(),
            request_type: RequestType::PresenceMemberLeft,
            channel: Some(channel.to_string()),
            socket_id: Some(socket_id.to_string()),
            user_id: Some(user_id.to_string()),
            // Cluster presence fields
            user_info: None,
            timestamp: None,
            dead_node_id: None,
            target_node_id: None,
            reply_to: None,
            channels: None,
        };

        // Send without waiting for response (broadcast) - skip if single node
        if !self.should_skip_horizontal_communication().await {
            self.transport.publish_request(&request).await
        } else {
            Ok(())
        }
    }

    async fn broadcast_presence_update(
        &self,
        app_id: &str,
        channel: &str,
        user_id: &str,
        socket_id: &str,
        user_info: sonic_rs::Value,
    ) -> Result<()> {
        self.horizontal
            .update_presence_entry(
                &self.node_id,
                channel,
                socket_id,
                user_id,
                app_id,
                user_info.clone(),
            )
            .await;

        if !self.cluster_health_enabled {
            return Ok(());
        }

        let request = RequestBody {
            request_id: crate::horizontal_adapter::generate_request_id(),
            node_id: self.node_id.clone(),
            app_id: app_id.to_string(),
            request_type: RequestType::PresenceMemberUpdated,
            channel: Some(channel.to_string()),
            socket_id: Some(socket_id.to_string()),
            user_id: Some(user_id.to_string()),
            user_info: Some(user_info),
            timestamp: None,
            dead_node_id: None,
            target_node_id: None,
            reply_to: None,
            channels: None,
        };

        if !self.should_skip_horizontal_communication().await {
            self.transport.publish_request(&request).await
        } else {
            Ok(())
        }
    }
}
