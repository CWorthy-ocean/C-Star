import asyncio


async def foo() -> float:
    sleep_time = 1
    await asyncio.sleep(sleep_time)
    return sleep_time


async def main() -> None:
    stuff = [foo() for _ in range(10)]

    results = await asyncio.gather(*stuff)
    total_sleep = sum(results)
    print(f"total sleep time was: {total_sleep}!")


if __name__ == "__main__":
    asyncio.run(main())
