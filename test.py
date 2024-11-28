import asyncio
import random
import time
from collections import deque
from llama_index.core.workflow.handler import WorkflowHandler
from llama_index.core.workflow import (
    StartEvent,
    StopEvent,
    Workflow,
    step,
    Event,
    Context
)
from llama_index.llms.azure_openai import AzureOpenAI
from dotenv import load_dotenv
load_dotenv()



# Define one single workflow
class FirstEvent(Event):
    first_output: str

class SecondEvent(Event):
    second_output: str
    response: str

class ProgressEvent(Event):
    msg: str

class MyWorkflow(Workflow):
    @step
    async def step_one(self, ctx: Context, ev: StartEvent) -> FirstEvent:
        ctx.write_event_to_stream(ProgressEvent(msg="Step one is happening"))
        return FirstEvent(first_output="First step complete.")
    
    @step
    async def step_two(self, ctx: Context, ev: FirstEvent) -> SecondEvent:
        llm = AzureOpenAI(
        engine="gpt-4o-mini",
        model="gpt-4o-mini",
        temperature=0.0,
        max_tokens=5000,
    )
        generator = await llm.astream_complete(
            "Please give me the first 3 paragraphs of Moby Dick, a book in the public domain."
        )
        async for response in generator:
            # Allow the workflow to stream this piece of response
            ctx.write_event_to_stream(ProgressEvent(msg=response.delta))
        return SecondEvent(
            second_output="Second step complete, full response attached",
            response=str(response),
        )
    
    @step
    async def step_three(self, ctx: Context, ev: SecondEvent) -> StopEvent:
        ctx.write_event_to_stream(ProgressEvent(msg="Step three is happening"))
        return StopEvent(result="Workflow complete.")

# def to run workflow
async def run_workflow(input_text, workflow):
    """Runs a single workflow instance."""
    handler: WorkflowHandler = workflow.run(first_input=input_text)

    async for ev in handler.stream_events():
        if isinstance(ev, ProgressEvent):  # Make sure ProgressEvent is defined/imported
            # print(f"Workflow for '{input_text}': {ev.msg}")
            pass
    result = await handler
    return result



class WorkflowOrchestrator:
    def __init__(self, num_workflows):
        self.workflows = [MyWorkflow(timeout=30, verbose=False) for i in range(num_workflows)]
        self.message_queue = deque()
        self.available_workflows = set(range(num_workflows))
        self.total_processing_time = 0

    async def enqueue_message(self, message):
        self.message_queue.append(message)
        print(f"Enqueued message: {message}")
        if self.available_workflows:  # Wake up a waiting worker if available
            self.available_workflows.pop()

    async def process_messages(self, workflow_id):
        while True:
            if not self.message_queue:
               self.available_workflows.add(workflow_id)
               await asyncio.sleep(0)
               if workflow_id not in self.available_workflows: #another process has grab the message and workflow_id not available now
                continue
               elif not self.message_queue:
                continue
               else:
                self.available_workflows.pop()

            message = self.message_queue.popleft()
            print(f"Workflow {workflow_id} picked up message: {message}")
            
            start_time = time.time()
            result = await run_workflow(message, self.workflows[workflow_id])
            end_time = time.time()
            processing_time = end_time - start_time  # Calculate processing time
            self.total_processing_time += processing_time  # Sum processing time

            print(f"Workflow {workflow_id} completed processing message: {message}")

            # Check if all workflows are done
            if not self.message_queue and not self.available_workflows:
                print("All workflows have completed their tasks.")
                break

    async def finish_processing(self):
        return f"Total processing time for all messages: {self.total_processing_time:.2f} seconds"  # Print total time


async def main():
    

    num_workflows = 3
    orchestrator = WorkflowOrchestrator(num_workflows)

    # Create agent tasks
    agent_tasks = [
        orchestrator.process_messages(i) for i in range(num_workflows)
    ]

    # Enqueue some messages
    messages = ["what is llama_index", "how to use llama_index", "give some example of llama_index", "Message 4", "Message 5"]
    for message in messages:
        await orchestrator.enqueue_message(message)

    await asyncio.gather(*agent_tasks)  # keep the program running even when there is no message

if __name__ == "__main__":
    import time
    import asyncio
    loop = asyncio.get_event_loop()
    start_time = time.time()  # Start time measurement
    asyncio.run(main())
    end_time = time.time()  # End time measurement
    total_execution_time = end_time - start_time
    print(f"Total execution time: {total_execution_time:.2f} seconds")
