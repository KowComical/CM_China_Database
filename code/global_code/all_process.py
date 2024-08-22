import os
import time
from contextlib import contextmanager
import traceback
import sys
import argparse

env_path = '/data3/kow/CM_China_Database'
sys.path.append(os.path.join(env_path, 'code'))

# Import the workflows
import global_code.craw_workday as gw
import Aviation.aviation as aa
import Ground_Transport.ground_transport as gg
import Industry.industry as ii
import Power.power as pp
import Residential.residential as rr
import global_code.all_sum as ga
import global_code.auto_upload as gau

import global_code.draw_pic as dp

workflows = {
    "craw_workday": gw,
    "aviation": aa,
    "ground_transport": gg,
    "industry": ii,
    "power": pp,
    "residential": rr,
    "all_sum": ga,
    "auto_upload": gau,
    "draw_pic": dp
}


def main():
    parser = argparse.ArgumentParser(description="Execute specified workflow modules.")
    parser.add_argument('workflows', nargs='*', help='List of workflow names to execute. Example: chile us')

    args = parser.parse_args()
    if args.workflows:
        execute_selected_workflows(args.workflows)
    else:
        execute_selected_workflows()


@contextmanager
def timeit_context(task_name):
    start_time = time.time()
    yield
    elapsed_time = time.time() - start_time

    hours = int(elapsed_time // 3600)
    minutes = int((elapsed_time % 3600) // 60)
    seconds = int(elapsed_time % 60)

    time_str = ""
    if hours > 0:
        time_str += f"{hours} hour{'s' if hours > 1 else ''} "
    if minutes > 0 or hours > 0:  # Include minutes if hours are also present
        time_str += f"{minutes} minute{'s' if minutes != 1 else ''} "
    time_str += f"{seconds} second{'s' if seconds != 1 else ''}"

    print(f'##### [{task_name}] finished in {time_str} #####')


def execute_workflow(workflow_module, workflow_name):
    capitalized_workflow_name = standardize_format(workflow_name)
    print(f"##### [{capitalized_workflow_name}] started #####")
    with timeit_context(capitalized_workflow_name):
        try:
            workflow_module.main()
        except Exception as e:
            tb = traceback.format_exc()
            print()
            print(f"Error in {capitalized_workflow_name}: {e}")
            print(f"Traceback:\n{tb}")
    print()  # add a blank line after each workflow


def execute_selected_workflows(selected_workflows=None):
    for name, workflow in workflows.items():
        if selected_workflows is None or name in selected_workflows:
            execute_workflow(workflow, name)


def standardize_format(s):
    # Replacing underscores with spaces, then capitalizing each word
    return ' '.join(part.capitalize() for part in s.replace('_', ' ').split())


if __name__ == '__main__':
    main()
