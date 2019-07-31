import ray
from pathlib import Path
from glob import glob
from squash_air import RuleSquasher
ray.init()
rays = []

@ray.remote
def distributed_run(filename, output):
    return RuleSquasher(filename, output).load_data().squash()

def distribed_run_all_matching(matchme, output):
    filenames = glob(matchme)
    already_run = list(map(lambda s: Path(s).name, glob(output+'*')))
    for filename in filenames:
        if Path(filename).name not in already_run:
            print('appending', filename)
            rays.append(distributed_run.remote(filename, output))
            
        else:
            print("[WARNING] Already found target in destination path. Skipping {}".format(filename))
            print()

if __name__ == '__main__':
    distribed_run_all_matching('/home/ubuntu/data/*.xlsx', '/home/ubuntu/output/')
    for output in ray.get(rays):
        print('saving something')
        output.save()