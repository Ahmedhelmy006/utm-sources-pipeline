import asyncio
import aiohttp
import socket
import ssl
import sys
from config.headers import headers
from rich.console import Console

console = Console()

async def on_request_start(session, trace_config_ctx, params):
    console.print(f"[yellow]Starting request to:[/yellow] {params.url}")

async def on_request_exception(session, trace_config_ctx, params):
    console.print(f"[bold red]Socket Exception:[/bold red] {params.exception}")

async def test_connection(subscriber_id):
    url = f"https://app.kit.com/subscribers/{subscriber_id}/referrer_info"
    
    trace_config = aiohttp.TraceConfig()
    trace_config.on_request_start.append(on_request_start)
    trace_config.on_request_exception.append(on_request_exception)

    console.print(f"[bold cyan]Verifying DNS for app.kit.com...[/bold cyan]")
    try:
        ip = socket.gethostbyname('app.kit.com')
        console.print(f"[green]DNS OK:[/green] app.kit.com resolved to {ip}")
    except Exception as e:
        console.print(f"[red]DNS FAILED:[/red] {e}")

    console.print(f"\n[bold cyan]Initiating Request for sub {subscriber_id}...[/bold cyan]")
    
    # We force IPv4 and check SSL
    connector = aiohttp.TCPConnector(family=socket.AF_INET)
    
    async with aiohttp.ClientSession(connector=connector, trace_configs=[trace_config]) as session:
        try:
            async with session.get(url, headers=headers, timeout=15) as response:
                console.print(f"[bold green]HTTP Status:[/bold green] {response.status}")
                if response.status == 200:
                    data = await response.json()
                    console.print("[green]Response Data received successfully![/green]")
                    console.print(data)
                else:
                    text = await response.text()
                    console.print(f"[red]Error body:[/red] {text}")
        except Exception as e:
            console.print(f"[bold red]Final Catch Exception:[/bold red] {type(e).__name__}: {e}")

if __name__ == "__main__":
    target_id = 3939215565
    asyncio.run(test_connection(target_id))