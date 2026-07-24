# dlb.tape — renders the README demo GIF deterministically.
#
#   brew install vhs        # one-time (pulls ttyd + ffmpeg)
#   vhs demo/dlb.tape       # run from the repo root -> writes docs/demo.gif
#
# Every take is identical: reset.sh wipes a scratch store first, and vhs types
# the exact same keystrokes. Re-render any time the API changes. The calls hit
# the real DLB store (via demo/agent.py); only the printed lines are tidied.

Output docs/demo.gif

Set Shell "bash"
Set FontSize 18
Set Width 1240
Set Height 620
Set Padding 24
Set TypingSpeed 45ms
Set Theme "Dracula"

# --- hidden setup: scratch store, venv, land in the demo dir ---
Hide
Type "bash demo/reset.sh >/dev/null 2>&1 && source .venv/bin/activate && cd demo && export DLB_STORE=/tmp/dlb-demo.sqlite3 && clear" Enter
Type "python -i -c 'from agent import Agent, task_status'" Enter
Sleep 600ms
Show

# --- the story: two agent sessions, one shared inbox ---
Type "# two agent sessions, one shared DLB inbox" Enter
Sleep 700ms
Type "backend = Agent('backend')" Enter
Sleep 700ms
Type "frontend = Agent('frontend')" Enter
Sleep 900ms

Type `task = backend.send('frontend', 'API v2 is live, migrate when you can', task=True)` Enter
Sleep 1300ms
Type "msgs = frontend.inbox()" Enter
Sleep 1300ms
Type "frontend.done(msgs[0].id, 'migrated')" Enter
Sleep 1300ms

# the payoff: the sender knows it landed, no token needed
Type "task_status(task)" Enter
Sleep 2800ms
