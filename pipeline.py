from stage import Stage
from util import Result

class Pipeline:
    
    '''
    async pipeline
    '''

    def __init__(self) -> None:
        self.stages: list[Stage] = []

    def Stage(self, st: Stage) -> "Pipeline":
        return self
    
    def run(self) -> Result:
        return True