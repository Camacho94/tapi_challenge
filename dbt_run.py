"""
dbt_run.py — Wrapper para ejecutar comandos dbt sin necesitar dbt en el PATH.

Uso:
    python dbt_run.py run
    python dbt_run.py snapshot
    python dbt_run.py test
    python dbt_run.py deps
    python dbt_run.py run --select fact_payments
    python dbt_run.py run --select marts
    python dbt_run.py run --full-refresh
"""

import sys
import os
from dbt.cli.main import dbtRunner

args = sys.argv[1:] + ["--project-dir", "dbt", "--profiles-dir", "dbt"]
result = dbtRunner().invoke(args)
os._exit(0 if result.success else 1)
