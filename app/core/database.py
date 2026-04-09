from typing import Annotated

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import NullPool
from sqlmodel.ext.asyncio.session import AsyncSession
from fastapi import Depends
from app.core.settings import settings

engine = create_async_engine(
    settings.database_url_async, 
    echo=settings.DB_ECHO,
    poolclass=NullPool
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine, 
    class_=AsyncSession,
    expire_on_commit=False
)


async def get_session():
    async with AsyncSessionLocal() as session:
        yield session

SessionDep = Annotated[AsyncSession, Depends(get_session)]