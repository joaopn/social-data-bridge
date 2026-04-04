"""pexpect-based wrapper for driving sdp.py interactively.

Spawns sdp.py with --tag mode, matches [tag_id] prompts, and sends
answers from a provided dict. Non-interactive commands (run, db start/stop)
use subprocess directly.
"""

import os
import subprocess
from pathlib import Path

import pexpect


WORKSPACE = Path("/workspace")

# Timeout for interactive commands (seconds)
INTERACTIVE_TIMEOUT = 120

# Timeout for pipeline commands that involve docker compose (seconds)
PIPELINE_TIMEOUT = 600


class SDPSession:
    """Drive sdp.py interactively via pexpect, answering prompts by tag."""

    def __init__(self, answers, extra_env=None):
        """
        Args:
            answers: Dict of {tag: answer_string}. Empty string = accept default.
            extra_env: Optional dict of extra environment variables.
        """
        self.answers = answers
        self.extra_env = extra_env or {}

    def run_interactive(self, cmd):
        """Run an interactive sdp.py command, answering tagged prompts.

        Args:
            cmd: Command string after "python sdp.py --tag", e.g. "db setup"

        Returns:
            (exit_status, output_text)
        """
        env = {**os.environ, "SDP_TAGGED_MODE": "1", "PYTHONUNBUFFERED": "1"}
        env.update(self.extra_env)

        full_cmd = f"python sdp.py --tag {cmd}"
        child = pexpect.spawn(
            "/bin/bash", ["-c", full_cmd],
            cwd=str(WORKSPACE),
            env=env,
            timeout=INTERACTIVE_TIMEOUT,
            encoding="utf-8",
            maxread=65536,
        )

        # Tags appear as "  [tag_id] prompt text" at the start of a line.
        # The regex must not match default values like [./data] or [Y/n].
        # Tags are always word-only (\w+) preceded by start-of-line + optional spaces.
        tag_pattern = r"(?:^|\n)\s*\[([a-z]\w*)\]"

        output_parts = []
        while True:
            try:
                index = child.expect([tag_pattern, pexpect.EOF])
                output_parts.append(child.before or "")
                if index == 1:  # EOF
                    break
                tag = child.match.group(1)
                answer = self.answers.get(tag, "")
                child.sendline(answer)
            except pexpect.TIMEOUT:
                output_parts.append(child.before or "")
                child.close(force=True)
                raise TimeoutError(
                    f"Timed out waiting for prompt.\n"
                    f"Last output:\n{''.join(output_parts[-3:])}"
                )

        child.close()
        return child.exitstatus, "".join(output_parts)


def run_sdp(cmd, extra_env=None, timeout=PIPELINE_TIMEOUT):
    """Run a non-interactive sdp.py command via subprocess.

    Args:
        cmd: Command string after "python sdp.py", e.g. "db start postgres"
        extra_env: Optional dict of extra environment variables.
        timeout: Command timeout in seconds.

    Returns:
        subprocess.CompletedProcess
    """
    env = {**os.environ}
    if extra_env:
        env.update(extra_env)

    return subprocess.run(
        ["python", "sdp.py"] + cmd.split(),
        cwd=WORKSPACE,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def wait_for_healthy(service, timeout=60):
    """Wait for a docker compose service to be healthy.

    Args:
        service: Service name (e.g. "postgres", "mongo")
        timeout: Max wait time in seconds.
    """
    import time
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = subprocess.run(
            ["docker", "compose", "ps", "--format", "{{.Health}}", service],
            cwd=WORKSPACE,
            capture_output=True,
            text=True,
        )
        if "healthy" in result.stdout:
            return
        time.sleep(2)
    raise TimeoutError(f"Service '{service}' did not become healthy within {timeout}s")
