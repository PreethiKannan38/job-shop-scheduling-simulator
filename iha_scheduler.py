# iha_scheduler.py
"""
Improved Hungarian Algorithm (IHA) Scheduler
--------------------------------------------
This module computes optimal job-machine assignments based on
current system conditions (job duration + machine workload).
"""

from typing import List, Tuple, Any
import math

try:
    import numpy as np
except Exception:
    np = None

try:
    from scipy.optimize import linear_sum_assignment
    _HAS_SCIPY = True
except Exception:
    linear_sum_assignment = None
    _HAS_SCIPY = False


# ---------------- Utility Functions ---------------- #

def _safe_array_min_max(arr):
    flat = [float(x) for x in arr if x is not None and not math.isinf(x)]
    if not flat:
        return 0.0, 0.0
    return min(flat), max(flat)


def _normalize_matrix(mat, eps=1e-9):
    if np is not None and isinstance(mat, np.ndarray):
        mn = float(np.nanmin(mat))
        mx = float(np.nanmax(mat))
        if abs(mx - mn) < eps:
            return np.zeros_like(mat)
        return (mat - mn) / (mx - mn)
    else:
        mn, mx = _safe_array_min_max([v for row in mat for v in row])
        if abs(mx - mn) < eps:
            return [[0.0 for _ in row] for row in mat]
        return [[(v - mn) / (mx - mn) for v in row] for row in mat]


def _greedy_assignment(cost_matrix, n_rows, n_cols):
    taken_rows, taken_cols, pairs = set(), set(), []
    entries = []

    if np is not None and isinstance(cost_matrix, np.ndarray):
        it = np.ndenumerate(cost_matrix)
        for (i, j), v in it:
            entries.append((float(v), i, j))
    else:
        for i, row in enumerate(cost_matrix):
            for j, v in enumerate(row):
                entries.append((float(v), i, j))

    entries.sort(key=lambda x: x[0])
    for cost, i, j in entries:
        if i in taken_rows or j in taken_cols:
            continue
        taken_rows.add(i)
        taken_cols.add(j)
        pairs.append((i, j))
        if len(taken_rows) >= n_rows or len(taken_cols) >= n_cols:
            break

    rows = [p[0] for p in pairs]
    cols = [p[1] for p in pairs]
    return rows, cols


# ---------------- Main IHA Function ---------------- #

def run_iha(
    jobs: List[Any],
    machines: List[Any],
    weights: Tuple[float, float] = (0.6, 0.4),
    eps: float = 99.0,
):
    """
    Perform Improved Hungarian Algorithm (IHA) scheduling.
    Uses two objectives:
      1️⃣ Flow-time (job.remaining_ticks_on_step)
      2️⃣ Workload  (machine.temperature + machine.vibration)

    Returns a list of (job, machine) pairs.
    """
    job_list = list(jobs)
    mach_list = list(machines)
    n_jobs = len(job_list)
    n_machs = len(mach_list)

    if n_jobs == 0 or n_machs == 0:
        return []

    k = max(n_jobs, n_machs)  # ensure square matrix
    if np is not None:
        L1 = np.full((k, k), eps, dtype=float)
        L2 = np.full((k, k), eps, dtype=float)
    else:
        L1 = [[eps] * k for _ in range(k)]
        L2 = [[eps] * k for _ in range(k)]

    # Build cost matrices
    for i, job in enumerate(job_list):
        for j, mach in enumerate(mach_list):
            try:
                t_flow = float(getattr(job, "remaining_ticks_on_step", 0.0))
            except Exception:
                t_flow = 0.0
            try:
                temp = float(getattr(mach, "temperature", 0.0))
            except Exception:
                temp = 0.0
            try:
                vib = float(getattr(mach, "vibration", 0.0))
            except Exception:
                vib = 0.0

            workload = temp + vib
            if np is not None:
                L1[i, j] = t_flow
                L2[i, j] = workload
            else:
                L1[i][j] = t_flow
                L2[i][j] = workload

    # Normalize
    L1n = _normalize_matrix(L1)
    L2n = _normalize_matrix(L2)

    w1, w2 = float(weights[0]), float(weights[1])
    wsum = (w1 + w2) if (w1 + w2) != 0 else 1.0
    w1 /= wsum
    w2 /= wsum

    if np is not None and isinstance(L1n, np.ndarray):
        L = w1 * L1n + w2 * L2n
    else:
        L = [
            [w1 * L1n[i][j] + w2 * L2n[i][j] for j in range(k)]
            for i in range(k)
        ]

    # Solve assignment
    if _HAS_SCIPY and np is not None:
        try:
            row_ind, col_ind = linear_sum_assignment(L)
        except Exception:
            row_ind, col_ind = _greedy_assignment(L, k, k)
    else:
        row_ind, col_ind = _greedy_assignment(L, k, k)

    assignments = []
    for r, c in zip(row_ind, col_ind):
        if r < n_jobs and c < n_machs:
            assignments.append((job_list[r], mach_list[c]))

    return assignments


# Optional test
if __name__ == "__main__":
    class MockJob:
        def __init__(self, jid, rem): self.job_id, self.remaining_ticks_on_step = jid, rem
        def __repr__(self): return f"{self.job_id}(rem={self.remaining_ticks_on_step})"
    class MockMach:
        def __init__(self, mid, t, v): self.machine_id, self.temperature, self.vibration = mid, t, v
        def __repr__(self): return f"{self.machine_id}(T={self.temperature},V={self.vibration})"

    jobs = [MockJob("J1", 4), MockJob("J2", 2), MockJob("J3", 5)]
    machs = [MockMach("M1", 30, 1.2), MockMach("M2", 50, 0.8)]
    res = run_iha(jobs, machs)
    print("\nAssignments:")
    for j, m in res:
        print("  ", j, "→", m)
