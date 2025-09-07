from stage import Stage
from util import Result, is_err, unwrap, Err
from typing import Callable, Any, Tuple
import asyncio
from asyncio import Queue
from worker import worker, SENTINEL

class Pipeline:
    
    '''
    async pipeline
    '''

    def __init__(self) -> None:
        self.log: bool = True
        self.gen_func: Callable[[], Tuple[bool, Result]] | None = None
        self.stages: list[Stage] = []
        self.result: Result = "ok"

    def gen(self, gen: Callable[[], Tuple[bool, Result]]) -> "Pipeline":
        self.gen_func = gen
        return self
    
    def stage(self, st: Stage) -> "Pipeline":
        if len(st.functions) > 0:
            self.stages.append(st)
        return self
    
    def __generate(self, gen: Callable[[], Tuple[bool, Result]]) -> Queue[Any]:
        outbound: Queue = Queue(1)
    
        async def run():

            con = True
            while con:
                try:
                    con, result = gen()
                except Exception as e:
                    self.__handle_err(str(e))
                    self.__handle_log(e)
                    await outbound.put(SENTINEL)
                    return    
                
                if not con:
                    await outbound.put(SENTINEL)
                
                if is_err(result):
                    self.__handle_err(str(result))
                    self.__handle_log(result)
                    await outbound.put(SENTINEL)
                    con = False      
                
                await outbound.put(unwrap(result))

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

        if not self.gen_func:
            Err("no generator")
            self.__handle_err(Err.message)
            self.__handle_log(Err.message)
            return Err
        
        stream = self.__generate(self.gen_func)
        for stage in self.stages:
            stream = worker(stage.functions[0], stream) # TODO handle types of stages
        await self.__drain(stream)
        return self.result