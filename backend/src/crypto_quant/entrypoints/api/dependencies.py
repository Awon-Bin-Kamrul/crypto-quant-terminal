"""Request dependencies: the service container and operator authentication."""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from crypto_quant.bootstrap.api import ApiContainer, tokens_match

bearer = HTTPBearer(
    auto_error=False,
    scheme_name="operator",
    description="The deployment's CQ_API_TOKEN",
)


def get_container(request: Request) -> ApiContainer:
    container: ApiContainer = request.app.state.container
    return container


ContainerDep = Annotated[ApiContainer, Depends(get_container)]


async def require_operator(
    container: ContainerDep,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Security(bearer)],
) -> None:
    """Every route except health requires ``Authorization: Bearer <CQ_API_TOKEN>``."""
    expected = container.settings.token.get_secret_value()
    if credentials is None or not tokens_match(expected, credentials.credentials):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing or invalid bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )


__all__ = ["ContainerDep", "bearer", "get_container", "require_operator"]
