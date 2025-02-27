import asyncio

from rewire import Space, DependenciesModule, LoaderModule, LifecycleModule


async def main():
    async with Space().init().use():
        await LoaderModule.get().discover().load()
        await DependenciesModule.get().solve()

        await LifecycleModule.get().start()


if __name__ == "__main__":
    asyncio.run(main())
