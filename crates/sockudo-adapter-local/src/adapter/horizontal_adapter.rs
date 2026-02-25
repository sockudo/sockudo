#[derive(Debug, Clone)]
pub struct DeadNodeEvent {
    pub dead_node_id: String,
    pub orphaned_members: Vec<OrphanedMember>,
}

#[derive(Debug, Clone)]
pub struct OrphanedMember {
    pub app_id: String,
    pub channel: String,
    pub user_id: String,
    pub user_info: Option<sonic_rs::Value>,
}
