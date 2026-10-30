"""
Initialize Prefect blocks and variables.

Creates:
  * block  github-credentials  (GitHub token, stored as a secret)
  * block  github-repo         (repository + branch)
  * the default Variables used by the flow

Safe to run several times (idempotent).
"""

import asyncio
import os
import sys
import time

import httpx
from dotenv import load_dotenv
from prefect.variables import Variable
from prefect_github import GitHubCredentials, GitHubRepository

CREDENTIALS_BLOCK_NAME = "github-credentials"
REPOSITORY_BLOCK_NAME = "github-repo"

DEFAULT_VARIABLES = {
    "rf_n_estimators": "100",
    "rf_max_depth": "10",
    "rf_test_size": "0.2",
    "model_name": "iris_model",
    "model_version": "1",
}


def wait_for_prefect(timeout: int = 120) -> None:
    """Wait until the Prefect API answers (the server may still be starting)."""
    api_url = os.getenv("PREFECT_API_URL")
    if not api_url:
        return
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if httpx.get(f"{api_url.rstrip('/')}/health", timeout=3).status_code == 200:
                print(f"Prefect API is ready at {api_url}")
                return
        except httpx.HTTPError:
            pass
        time.sleep(2)
    sys.exit(f"ERROR: Prefect API at {api_url} not reachable after {timeout}s")


async def create_blocks():
    """Initialize all required Prefect blocks and variables."""
    load_dotenv()  # useful when running outside Docker

    # 1. GitHub credentials block (the token is stored as a secret)
    github_token = os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN")
    if not github_token:
        raise ValueError("GITHUB_TOKEN environment variable not set")
    credentials = GitHubCredentials(token=github_token)
    await credentials.save(name=CREDENTIALS_BLOCK_NAME, overwrite=True)

    # 2. GitHub repository block
    repository = os.getenv("GITHUB_REPOSITORY")
    if not repository:
        raise ValueError("GITHUB_REPOSITORY environment variable not set")
    github_repo = GitHubRepository(
        repository_url=f"https://github.com/{repository}",
        reference=os.getenv("GITHUB_BRANCH", "main"),
        credentials=credentials,
    )
    await github_repo.save(name=REPOSITORY_BLOCK_NAME, overwrite=True)
    print(f"Blocks saved: {CREDENTIALS_BLOCK_NAME}, {REPOSITORY_BLOCK_NAME}")

    # 3. Default variables (an existing value, e.g. edited in the UI, is kept)
    for name, value in DEFAULT_VARIABLES.items():
        if await Variable.get(name, default=None) is None:
            await Variable.set(name, value)
            print(f"Variable created: {name}={value}")
        else:
            print(f"Variable kept:    {name}")


if __name__ == "__main__":
    wait_for_prefect()
    asyncio.run(create_blocks())
