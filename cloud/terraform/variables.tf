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

variable "burst" {
  type    = number
  default = 10
}

variable "rate" {
  type    = number
  default = 5
}

variable "bootstrap_path" {
  type        = string
  default     = "../build/bootstrap"
  description = "Path to the compiled Go Lambda binary (linux/arm64, named bootstrap)."
}
