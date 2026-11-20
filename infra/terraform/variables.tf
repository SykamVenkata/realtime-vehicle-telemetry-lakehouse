variable "subscription_id" {
  description = "Azure subscription to deploy into"
  type        = string
}

variable "prefix" {
  description = "Short prefix for resource names (lowercase letters and digits)"
  type        = string
  default     = "vtl"
  validation {
    condition     = can(regex("^[a-z0-9]{2,8}$", var.prefix))
    error_message = "prefix must be 2-8 lowercase letters or digits."
  }
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "southcentralus"
}

variable "eventhub_partitions" {
  description = "Event Hub partitions (sets max parallelism of the Bronze stream)"
  type        = number
  default     = 4
}

variable "eventhubs_throughput_units" {
  description = "Event Hubs throughput units (1 TU = 1 MB/s or 1,000 events/s ingress)"
  type        = number
  default     = 1
}

variable "tags" {
  description = "Extra tags for every resource"
  type        = map(string)
  default     = {}
}
