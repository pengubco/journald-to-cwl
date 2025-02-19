# Journald-to-cwl

`journald-to-cwl` pushes Journald logs from EC2 instance to AWS Cloudwatch Logs (CWL). In production, you'd run it as 
a systemd service. It works as a simple linear Extract -> Transform -> Load process. 

![](./design.svg)

Simplicity in the design leads to its robustness.

1. For simplicity, the CWL and the EC2 instance must be in the same AWS region and they belong to the same AWS account.

1. For simplicity, it does not provide configurations like filter log entry by priorities, entry keys, regular expressions. 
It literally pushes all journald logs to CWL. If you need to filter logs, for example, you cannot push logs with 
sensitive information to CWL, you can configure [systemd logging](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html#Logging%20and%20Standard%20Input/Output) directly.

1. For simplicity, it uses permissions from the EC2 instance profile. 

1. For simplicity, it does not take command flags. Instead, it takes an Env style configuration file. 
```
log_group = ""    # CWL log group name.
log_stream = ""   # CWL log stream name.
state_file = ""   # A text file that persist the state. 
```
The default configuration is,
```
log_group = "journal-logs"
state_file = "/var/lib/journald-to-cwl/state"
log_stream = "<instance-id>" # for example, "i-11111111111111111"
```

## Installaion
You can download go binary and RPM package from the Release page. You can also build it from source.
```sh
# install C headers.
yum install systemd-devel

# install Go and build the binary.
go mod tidy
make build-go

# install rpmbuild tools and build the RPM package
yum install rpmdevtools rpmlint
make build-rpm
```

Install 
```sh
# use the rpm you dowloaded or built.
sudo rpm -ivh journald-to-cwl-0.1.0-1.amzn2.x86_64.rpm
# make changes to the config file if needed
cat /etc/journald-to-cwl/journald-to-cwl.conf

sudo systemctl start journald-to-cwl
```

An log event example in CWL.
```json
{
    "instanceId": "i-11111111111111111",
    "realTimestamp": 1728886624050615,
    "pid": 10993,
    "uid": 1000,
    "gid": 0,
    "cmdName": "sudo",
    "exe": "/usr/bin/sudo",
    "systemdUnit": "session-1.scope",
    "bootId": "a1111111111111111111111111111111",
    "machineId": "b1111111111111111111111111111111",
    "hostname": "ip-101-01-01-01.us-west-2.compute.internal",
    "transport": "syslog",
    "priority": "info",
    "message": "pam_unix(sudo:session): session closed for user root",
    "syslog": {
        "facility": 10,
        "ident": "sudo"
    }
}
```

## Code structure
The design is as simple as a typical ETL and the implementation uses a root Context and two Go channels for coordination.
1. Extract (`journal/reader.go`): Reads journal entries into a channel `entries`.
2. Transform and Batching (`batch/batch.go`): Consume from the `entries` channel. Then, transfrom entries into log events 
that can be sent to CWL. Finally, batch events into a channel of `batches`.
3. Load (`cwl/writer.go`): Consume from the `batches` channel and send each batch to CWL.

## FAQ
Q1. Where does the logs of journald-to-cwl go? 

If you run it as a systemd service and do not set `StandardOutput` and `StandardError` in the `journald-to-cwl.service`, 
the logs go to journald and later read by `journald-to-cwl` itself. That's why journald-to-cwl does not log excessively. 
Otherwise, it may DOS itself. 

Q2. Why do I need journald-to-cwl?

You need to push journal logs to CWL and you don't want set up 'do-it-all' tools like [OTEL](https://opentelemetry.io/)
or [Vector](https://vector.dev/). Note that, the amazon-cloudwatch-agent(https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/Install-CloudWatch-Agent.html) does not support journal logs.

Q3. journald-to-cwl seems like doing the same thing as [journald-cloudwatch-logs](https://github.com/saymedia/journald-cloudwatch-logs). 
Why would I choose journald-to-cwl over journald-cloudwatch-logs?
The journald-cloudwatch-logs has not had any update since 2017. And there are a few problems for me to use it in production.
  1. Outdated SDK. The [aws-sdk-go](https://github.com/aws/aws-sdk-go?tab=readme-ov-file#warning-this-sdk-is-in-maintenance-mode) 
entered maintenance mode on 7/31/2024 and will enter end-of-support on 7/31/2025. In maintenance mode, the SDK will not 
receive API updates for new or existing services, or be updated to support new regions.
  2. It does build with Go1.22+. The latest commit relies on vendor. However, starting with 
[Go1.22](https://go.dev/doc/go1.22), `go mod init` no longer attempts to import module requirements from 
configuration files for other vendoring tools (such as Gopkg.lock).
  3. There is no release version
  4. There is no LICENSE, which is a risk running in production.
  5. There is not a single line of test yet many lines of TODO.  
