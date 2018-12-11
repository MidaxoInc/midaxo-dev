import pandas as pd
import numpy as np
data = datasets["ASP"]
data = data.fillna(np.nan)
data.head()