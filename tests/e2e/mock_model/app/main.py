"""Minimal KServe V2 echo model for E2E tests.

Returns every input tensor unchanged as output tensors.
No ML models, no external dependencies — pure pass-through.
"""

from moiraweave_model_sdk.base import BaseModelService
from moiraweave_model_sdk.models import InferRequest, InferResponse


class EchoModelService(BaseModelService):
    """Returns all input tensors unchanged as outputs."""

    @property
    def name(self) -> str:
        return "echo"

    @property
    def version(self) -> str:
        return "1"

    @property
    def task(self) -> str:
        return "text-passthrough"

    async def predict(self, request: InferRequest) -> InferResponse:
        return InferResponse(
            model_name=self.name,
            id=request.id,
            outputs=list(request.inputs),
        )


app = EchoModelService().build_app()
