from __future__ import annotations

from beartype import beartype
from beartype.typing import List
from common_package.process_logs.commands import WAITING_RESULT, Command, Result
from loguru import logger

from pyspeedy.patterns.observer import Observer, Subject


class ProccessNode(Subject):
    def __init__(self, command: Command, observer: Observer = None):
        self.command: Command = command
        self.result: Result = Result()
        self.observers: List[Observer] = []
        if isinstance(observer, Observer):
            self.attach(observer)
        self.next: List[ProccessNode] = []
        self.rank: int = None

    @beartype
    def attach(self, observer: Observer) -> None:
        self.observers.append(observer)

    @beartype
    def detach(self, observer: Observer) -> None:
        self.observers.remove(observer)

    def notify(self) -> None:
        for observer in self.observers:
            observer.update(self)

    def merge(self, node: ProccessNode) -> None:
        if node.command == self.command:
            self.observers = list(set(self.observers + node.observers))
            self.merge_childs(node)
        else:
            raise Exception("Cannot merge thesse nodes!!!")

    def merge_childs(self, node: ProccessNode) -> None:
        appended_child_nodes = list()
        for node_child in node.next:
            duplicate = False
            for self_child in self.next:
                if node_child.command == self_child.command:
                    duplicate = True
                    self_child.merge(node_child)
                    break

            if not duplicate:
                appended_child_nodes.append(node_child)

        self.next = self.next + appended_child_nodes

    @beartype
    def run(self, parent_result: Result = None) -> None:
        self.result.running()

        # check params need WAITING_RESULT and fill it
        for kw, arg in self.command.kwargs.items():
            if isinstance(arg, Result) and arg == WAITING_RESULT:
                assert (
                    parent_result is not None
                ), "Task which needs waiting result not must be passed None-parent_result!!!"
                self.command.kwargs[kw] = parent_result.result
        self.command.args = list(self.command.args)
        for idx, arg in enumerate(self.command.args):
            if isinstance(arg, Result) and arg == WAITING_RESULT:
                assert (
                    parent_result is not None
                ), "Task which needs waiting result not must be passed None-parent_result!!!"
                self.command.args[idx] = parent_result.result

        self.result = self.command.run()
        self.notify()
        if self.result.is_done():
            for node in self.next:
                node.run(self.result)
        elif self.result.is_error():
            for node in self.next:
                node.notify_failure(self)

        self.next = []

    def notify_failure(self, parent_node: ProccessNode) -> None:
        self.result = parent_node.result
        self.notify()
        for node in self.next:
            node.notify_failure(self)

    def __str__(self) -> str:
        ret = "----|" * self.rank + self.command.func.__name__ + "\n"
        for child in self.next:
            ret += child.__str__()
        return ret


class RootProccessNode(ProccessNode):
    def __init__(self):
        super().__init__(command=Command(logger.info, "Run kernel..."))
        self.rank = 0
