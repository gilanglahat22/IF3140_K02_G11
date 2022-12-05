from typing import NamedTuple, Literal


class Action(NamedTuple):
    operation: Literal["write", "read", "commit", "abort"]
    time: int
    value: str | None = None

    def print(self, transaction_id: int):
        if self.operation == "write":
            print(f"W{transaction_id}({self.value});")
        if self.operation == "read":
            print(f"R{transaction_id}({self.value});")
        if self.operation == "commit":
            print(f"C{transaction_id};")
        if self.operation == "abort":
            print(f"A{transaction_id};")


def build_action(action_str: str, time: int) -> Action:
    if action_str[0] == "C":
        return Action("commit", time)
    elif action_str[0] == "R":
        value = action_str.split("(")[1].split(")")[0]
        return Action("read", time, value)
    elif action_str[0] == "W":
        value = action_str.split("(")[1].split(")")[0]
        return Action("write", time, value)
    elif action_str[0] == "A":
        return Action("abort", time)


class Transaction:
    def __init__(self, number, startTS):
        self.number = number
        self.startTS = startTS
        self.validationTS = 0
        self.finishTS = 10 ** 8
        self.actions: list[Action] = []
        self.state = 0
        self.read_sets = set()
        self.write_sets = set()
        self.is_end = False

    def add_action(self, action: Action):
        self.actions.append(action)

        if (action.operation == "write"):
            self.write_sets.add(action.value)
        elif (action.operation == "read"):
            self.read_sets.add(action.value)

    def get_current_action(self) -> Action:
        return self.actions[self.state]

    def increment_state(self):
        self.state += 1

        if self.state == len(self.actions):
            self.is_end = True
