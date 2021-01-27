resource "aws_sns_topic" "notification_topic" {
  name = var.topic_name
  display_name = "The notification topic for the ESP Analytics project"
  tags = var.sns_tags
}
