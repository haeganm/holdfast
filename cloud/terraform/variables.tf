variable "region" {
  type    = string
  default = "us-east-1"
}

variable "name" {
  type    = string
  default = "holdfast"

  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.name))
    error_message = "name may only contain lowercase letters, digits and hyphens."
  }
}

variable "peer_token" {
  type      = string
  sensitive = true

  validation {
    condition     = length(var.peer_token) >= 32
    error_message = "peer_token must be at least 32 characters."
  }
}

variable "alert_email" {
  type = string

  validation {
    condition     = can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", var.alert_email))
    error_message = "alert_email must look like an email address."
  }
}

variable "budget_usd" {
  type    = string
  default = "5"
}

# Three edges syncing every second make 3 to 6 requests a second. The
# throttle is there to cap what a stranger with the URL can run up, not to
# slow the edges down.
variable "burst" {
  type    = number
  default = 20
}

variable "rate" {
  type    = number
  default = 10
}

# Per second. Three live edges need about 100 reads (each request scans the
# table, 1 unit per 4 KB) and bursts of up to about 100 writes after a poll.
variable "max_read_units" {
  type    = number
  default = 300
}

variable "max_write_units" {
  type    = number
  default = 200
}

variable "bootstrap_path" {
  type        = string
  default     = "../build/bootstrap"
  description = "Path to the compiled Go Lambda binary (linux/arm64, named bootstrap)."
}
