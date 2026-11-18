Name: journald-to-cwl
Version: 1.0.0
Release: 1%{?dist}
Summary: Push journal logs to CloudWatch Logs
License: MIT

Source0: journald-to-cwl
Source1: journald-to-cwl.service
Source2: journald-to-cwl.conf

%description
%{summary}.

%prep

%build

%install
install -d %{buildroot}%{_bindir}
install -D -p -m 0755 %{S:0} %{buildroot}%{_bindir}
install -D -p -m 0644 %{S:1} %{buildroot}%{_unitdir}/journald-to-cwl.service

install -d %{buildroot}%{_sysconfdir}/journald-to-cwl/
install -m 0644 %{S:2} %{buildroot}%{_sysconfdir}/journald-to-cwl/journald-to-cwl.conf

install -d %{buildroot}%{_sharedstatedir}/journald-to-cwl
touch %{buildroot}%{_sharedstatedir}/journald-to-cwl/state

%files
%{_bindir}/journald-to-cwl
%{_unitdir}/journald-to-cwl.service
%dir %{_sysconfdir}/journald-to-cwl
%{_sysconfdir}/journald-to-cwl/journald-to-cwl.conf
%dir %{_sharedstatedir}/journald-to-cwl
%{_sharedstatedir}/journald-to-cwl/state

%changelog
* Wed Nov 18 2026 Peng Zhang <pengubco@users.noreply.github.com> - 1.0.0-1
- Retry network errors and CloudWatch Logs server errors for up to 15 minutes per batch.
- Retry IMDS at startup.
- Start a new batch when the clock goes backwards.
- Add skip_audit_log and ignore_patterns.
