from pyspeedy.common.utils import is_notebook

if is_notebook():
    from tqdm.notebook import tqdm
else:
    from pyspeedy.common.tqdm import tqdm
