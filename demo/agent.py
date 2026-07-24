"""Readable wrapper over dlb_mcp.store, used only to render the README demo GIF.

Every method calls the real DLB store API; the prints are just tidied so the
recording reads cleanly. This is demo scaffolding, not part of the shipped
package.
"""

from __future__ import annotations

import dlb_mcp.store as store


class Agent:
    """One named agent session. Registers on construction and remembers its
    token, the same way a real MCP session would."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.token = store.register(name)["session_token"]

    def send(self, to: str, body: str, task: bool = False) -> int:
        msg = store.send(
            to=to,
            body=body,
            from_=self.name,
            session_token=self.token,
            msg_type="task" if task else None,
        )
        tag = " [task]" if task else ""
        print(f"  {self.name} -> {to}{tag}: {body}")
        return msg.id

    def inbox(self) -> list:
        msgs = store.read(self.name, self.token)
        for m in msgs:
            tag = " [task]" if m.msg_type == "task" else ""
            print(f"  {self.name} inbox #{m.id} from {m.sender_name}{tag}: {m.body}")
        return msgs

    def done(self, message_id: int, note: str) -> None:
        store.update_status(message_id, "done", self.token, note=note)
        print(f"  {self.name} done #{message_id}: {note}")


def task_status(message_id: int) -> None:
    """Sender-side probe, no token needed. This is the line a chat box in
    another terminal can never give you."""
    s = store.get_task_status(message_id)
    print(f"  status #{message_id}: {s['status']} ({s['status_note']})")
