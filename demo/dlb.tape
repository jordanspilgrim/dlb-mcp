# dlb.tape — two-terminal (tmux split) demo, rendered deterministically.
#
#   brew install vhs        # one-time (pulls ttyd + ffmpeg)
#   vhs demo/dlb.tape       # run from the repo root -> writes docs/demo.gif
#
# Left pane = a "backend" agent session, right pane = a "frontend" session,
# two independent Python REPLs sharing one DLB store. reset.sh wipes a scratch
# store first so every take is identical. The calls hit the real DLB store
# (via demo/agent.py); only the printed lines are tidied.

Output docs/demo.gif

Set Shell "bash"
Set FontSize 16
Set Width 1500
Set Height 260
Set Padding 20
Set TypingSpeed 45ms
Set Theme "Dracula"

# --- hidden setup: scratch store, two labelled tmux panes, a REPL in each ---
Hide
Type "bash demo/reset.sh >/dev/null 2>&1 && export DLB_STORE=/tmp/dlb-demo/store.sqlite3 && cd demo && source ../.venv/bin/activate && clear" Enter
# Only ever touch OUR named session, never kill-server (that would kill your other tmux sessions).
Type "tmux kill-session -t dlbdemo 2>/dev/null; tmux new-session -d -s dlbdemo" Enter
Type "tmux set -g status off; tmux set -g pane-border-status top; tmux set -g pane-border-format ' #{pane_title} '" Enter
Type "tmux set -g pane-border-style fg=brightblack; tmux set -g pane-active-border-style fg=brightblack" Enter
Type "tmux split-window -h -t dlbdemo" Enter
Type "tmux select-pane -t dlbdemo.0 -T 'backend terminal'" Enter
Type "tmux select-pane -t dlbdemo.1 -T 'frontend terminal'" Enter
Type `tmux send-keys -t dlbdemo.0 'PS1="$ "; clear; python -i -c "from agent import Agent, task_status"' Enter` Enter
Type `tmux send-keys -t dlbdemo.1 'PS1="$ "; clear; python -i -c "from agent import Agent, task_status"' Enter` Enter
Type "clear; tmux attach -t dlbdemo" Enter
Sleep 1500ms
Show

# --- left pane (backend): register, then hand off a task ---
Ctrl+b
Sleep 200ms
Left
Sleep 400ms
Type "backend = Agent('backend')" Enter
Sleep 700ms
Type `task = backend.send('frontend', 'migrate to /api/v2', task=True)` Enter
Sleep 1300ms

# --- right pane (frontend): register, read it, mark it done ---
Ctrl+b
Sleep 200ms
Right
Sleep 400ms
Type "frontend = Agent('frontend')" Enter
Sleep 700ms
Type "msgs = frontend.inbox()" Enter
Sleep 1200ms
Type "frontend.done(msgs[0].id, 'migrated')" Enter
Sleep 1300ms

# --- back to backend: did it land? no token needed ---
Ctrl+b
Sleep 200ms
Left
Sleep 400ms
Type "task_status(task)" Enter
Sleep 2800ms

# --- hidden teardown: detach and drop our session ---
Hide
Ctrl+b
Sleep 150ms
Type "d"
Sleep 300ms
Type "tmux kill-session -t dlbdemo 2>/dev/null" Enter
