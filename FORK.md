# Forgeplane's go-plugin fork

This is Forgeplane's fork of [hashicorp/go-plugin](https://github.com/hashicorp/go-plugin), based on v1.8.0 (`155dcddc9`). It carries exactly two patches:

1. Close broker listeners when address translation or advertisement fails, preventing leaked listener descriptors and Unix socket files when a plugin exits during channel setup.
2. Route broker errors through the owning client's or server's hclog logger instead of the process-global standard logger, so embedders can route or silence errors (including socket paths) per instance.

The module path remains `github.com/hashicorp/go-plugin`. Consumers use a `replace` directive to select this fork without changing imports.

This fork will be dropped when upstream fixes both issues. The upstream MPL-2.0 license is unchanged.
