import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import mlflow
from prefect.testing.utilities import prefect_test_harness
from prefect.context import get_run_context
from flows.iris_flow_mlflow import iris_classification_flow, setup_variables

@pytest.mark.integration
def test_end_to_end_flow():
    # Set up variables first
    setup_variables()
    
    with prefect_test_harness():
        with mlflow.start_run(nested=True):
            result = iris_classification_flow()
            assert result is not None

@pytest.mark.integration
def test_docker_deployment():
    import subprocess
    
    # Check if the required images exist
    required_images = [
        "prefect-iris-classification-mlflow:latest",
        "prefect-iris-classification-prefect-server:latest"
    ]
    
    # Get list of Docker images
    result = subprocess.run(["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"], 
                          capture_output=True, text=True)
    images = result.stdout.strip().split('\n')
    
    for required_image in required_images:
        assert any(required_image in img for img in images), f"Required image {required_image} not found"