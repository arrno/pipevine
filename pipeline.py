from stage import Stage
from util import Result, is_err, unwrap, Err
from typing import Any, Iterator
import asyncio
from asyncio import Queue, shield
from worker import worker, SENTINEL

class Pipeline:
    
    '''
    async pipeline
    '''

    def __init__(self, gen: Iterator[Any]) -> None:
        self.log: bool = True
        self.generator: Iterator[Any] | None = None
        self.stages: list[Stage] = []
        self.result: Result = "ok"
        self.gen(gen)

    def gen(self, gen: Iterator[Any]) -> "Pipeline":
        self.generator = gen
        return self
    
    def stage(self, st: Stage) -> "Pipeline":
        if len(st.functions) > 0:
            self.stages.append(st)
        return self
    
    def __generate(self, gen: Iterator[Any]) -> asyncio.Queue[Any]:
        outbound: asyncio.Queue[Any] = asyncio.Queue(maxsize=1)

        async def run() -> None:
            try:
                for result in gen:
                    if is_err(result):
                        self.__handle_err(str(result))
                        self.__handle_log(result)
                        return                        # sentinel sent in finally
                    await outbound.put(unwrap(result))
            except asyncio.CancelledError:
                # allow task cancellation to propagate; finally still runs
                raise
            except Exception as e:
                # real error from iterator or unwrap()
                self.__handle_err(str(e))
                self.__handle_log(e)
            finally:
                # guarantee exactly-once termination signal
                try:
                    await shield(outbound.put(SENTINEL))
                except Exception:
                    pass

        asyncio.create_task(run())
        return outbound
    
    def __handle_log(self, val: Any) -> None:
        if self.log:
            print(val)
    
    def __handle_err(self, err: str) -> None:
        self.result = Err(err)

    async def __drain(self, q: Queue[Any]) -> None:
        while True:
            val = await q.get()
            if val is SENTINEL:
                break
            self.__handle_log(val)
            
    async def run(self) -> Result:

        if not self.generator:
            Err("no generator")
            self.__handle_err(Err.message)
            self.__handle_log(Err.message)
            return Err
        
        stream = self.__generate(self.generator)
        for stage in self.stages:
            stream = worker(stage.functions[0], stream) # TODO handle types of stages
        await self.__drain(stream)
        return self.result