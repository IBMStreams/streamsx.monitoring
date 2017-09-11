---
title: "Migration Document"
permalink: /docs/user/migration/
excerpt: "Migration from Metrics Toolkit to Monitoring Toolkit"
last_modified_at: 2017-09-11T12:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}

Developers with applications using the MetricsSource operator from `com.ibm.streamsx.metrics` toolkit are encouraged to migrate to the new `com.ibm.streamsx.monitoring` toolkit. Use the following guidelines to migrate applications to the new toolkit. 

### Toolkit Changes

| (Previous) Metrics Toolkit | (New) Monitoring Toolkit | Additional Information |
| --- | --- | --- | 
| Old namespace: `com.ibm.streamsx.metrics` | New namespace: `com.ibm.streamsx.monitoring` | Applications need to replace the `com.ibm.streamsx.metrics` namespace with `com.ibm.streamsx.monitoring.metrics` for the MetricsSource operator. |
