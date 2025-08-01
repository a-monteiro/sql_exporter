# sql-exporter

![Version: 0.12.4](https://img.shields.io/badge/Version-0.12.4-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 0.18.0](https://img.shields.io/badge/AppVersion-0.18.0-informational?style=flat-square)

Database-agnostic SQL exporter for Prometheus

## Source Code

* <https://github.com/burningalchemist/sql_exporter>

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| Nikolai Rodionov | <allanger@zohomail.com> | <https://badhouseplants.net> |

## Installing the Chart

To install the chart with the release name `sql-exporter`:

```console
helm repo add sql_exporter https://burningalchemist.github.io/sql_exporter/
helm install sql_exporter/sql-exporter
```

### Ingress support

It's possible to enable the ingress creation by setting

```yaml
#Values
ingress:
  enabled: true
```

But as the sql_operator has a direct connection to databases,
it might expose the database servers to possible DDoS attacks.
It's not recommended by maintainers to use ingress for accessing the exporter,
but if there are no other options,
security measures should be taken.

For example, a user might enable the basic auth on the ingress level.
Take a look on how it's done at the
[nginx ingress controller](https://kubernetes.github.io/ingress-nginx/examples/auth/basic/)
as an example.

## Chart Values

### General parameters

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| nameOverride | string | `""` | Provide a name in place of `sql-exporter` |
| fullnameOverride | string | `""` | String to fully override "sql-exporter.fullname" |
| image.repository | string | `"burningalchemist/sql_exporter"` | Image repository |
| image.pullPolicy | string | `"IfNotPresent"` | Image pull policy |
| image.tag | string | `appVersion` value from `Chart.yaml` | Image tag |
| imagePullSecrets | list | `[]` | Secrets with credentials to pull images from a private registry |
| service.type | string | `"ClusterIP"` | Service type |
| service.port | int | `80` | Service port |
| service.labels | object | `{}` | Service labels |
| service.annotations | object | `{}` | Service annotations |
| ingress.enabled | bool | `false` |  |
| ingress.labels | object | `{}` | Ingress labels |
| ingress.annotations | object | `{}` | Ingress annotations |
| ingress.ingressClassName | string | `""` | Ingress class name |
| ingress.host | string | `""` | Ingress host |
| ingress.path | string | `"/"` | Ingress path |
| ingress.tls | object | `{"crt":"","enabled":false,"key":"","secretName":""}` | Ingress TLS, can be defined by cert secret, or by key and cert. |
| ingress.tls.secretName | string | `""` | Ingress tls secret if already exists. |
| ingress.tls.crt | string | `""` | Ingress tls.crt, required if you don't have secret name. |
| ingress.tls.key | string | `""` | Ingress tls.key, required if you don't have secret name. |
| extraContainers | object | `{}` | Arbitrary sidecar containers list |
| initContainers | object | `{}` | Arbitrary sidecar containers list for 1.29+ kubernetes |
| serviceAccount.create | bool | `true` | Specifies whether a Service Account should be created, creates "sql-exporter" service account if true, unless overriden. Otherwise, set to `default` if false, and custom service account name is not provided. Check all the available parameters. |
| serviceAccount.annotations | object | `{}` | Annotations to add to the Service Account |
| livenessProbe.initialDelaySeconds | int | `5` |  |
| livenessProbe.timeoutSeconds | int | `30` |  |
| readinessProbe.initialDelaySeconds | int | `5` |  |
| readinessProbe.timeoutSeconds | int | `30` |  |
| resources | object | `{}` | Resource limits and requests for the application controller pods |
| podLabels | object | `{}` | Pod labels |
| podAnnotations | object | `{}` | Pod annotations |
| podSecurityContext | object | `{}` | Pod security context |
| createConfig | bool | `true` | Set to true to create a config as a part of the helm chart |
| logLevel | string | `"debug"` | Set log level (info if unset) |
| logFormat | string | `"logfmt"` | Set log format (logfmt if unset) |
| reloadEnabled | bool | `false` | Enable reload collector data handler (endpoint /reload) |

### Prometheus ServiceMonitor

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| serviceMonitor.enabled | bool | `true` | Enable ServiceMonitor |
| serviceMonitor.interval | string | `"15s"` | ServiceMonitor interval |
| serviceMonitor.path | string | `"/metrics"` | ServiceMonitor path |
| serviceMonitor.metricRelabelings | object | `{}` | ServiceMonitor metric relabelings |
| serviceMonitor.relabelings | object | `{}` | ServiceMonitor relabelings |
| serviceMonitor.namespace | string | `nil` | ServiceMonitor namespace override (default is .Release.Namespace) |
| serviceMonitor.scrapeTimeout | string | `nil` | ServiceMonitor scrape timeout |

### Configuration

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| config | object | `{"global":{"max_connections":3,"max_idle_connections":3,"min_interval":"0s","scrape_error_drop_interval":"0s","scrape_timeout":"10s","scrape_timeout_offset":"500ms"}}` | SQL Exporter configuration, can be a dictionary, or a template yaml string. |
| config.global.scrape_timeout | string | `"10s"` | Scrape timeout |
| config.global.scrape_timeout_offset | string | `"500ms"` | Scrape timeout offset. Must be strictly positive. |
| config.global.scrape_error_drop_interval | string | `"0s"` | Interval between dropping scrape_errors_total metric: by default the metric is persistent. |
| config.global.min_interval | string | `"0s"` | Minimum interval between collector runs. |
| config.global.max_connections | int | `3` | Number of open connections. |
| config.global.max_idle_connections | int | `3` | Number of idle connections. |
| target | object | `nil` | Check documentation. Mutually exclusive with `jobs`  |
| jobs   | list | `nil` | Check documentation. Mutually exclusive with `target` |
| collector_files | list | `[]` | Check documentation |

To generate the config as a part of a helm release, please set the `.Values.createConfig` to true, and define a config under the `.Values.config` property.

To configure `target`, `jobs`, `collector_files` please refer to the [documentation](https://github.com/burningalchemist/sql_exporter/blob/master/documentation/sql_exporter.yml) in the source repository. These values are not set by default.

It's also possible to define collectors (i.e. metrics and queries) in separate files, and specify the filenames in the `collector_files` list. For that we can use `CollectorFiles` field (check `values.yaml` for the available example).

## Dev Notes

After changing default `Values`, please execute `make gen_docs` to update the `README.md` file. Readme file is generated by the `helm-docs` tool, so make sure not to edit it manually.
