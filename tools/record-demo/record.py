"""Record the lite-server WebUI demo as a high-res video.

Usage:
    ../../.venv/bin/python record.py [output_dir]

Produces out/<timestamp>.webm (Playwright), meant to be converted to
.assets/demo.mp4 with ffmpeg (see convert.sh).

Requires the BFF at http://127.0.0.1:8600 (admin / AcceptPass456!) and the
backend instance reachable, ideally with features.timeline on and some
predict traffic so the charts are alive.
"""

import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE = "http://127.0.0.1:8600"
OUT = Path(sys.argv[1] if len(sys.argv) > 1 else "out")

VIEWPORT = {"width": 1920, "height": 1200}


def beat(page, ms):
    page.wait_for_timeout(ms)


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport=VIEWPORT,
            device_scale_factor=2,
            locale="en-US",
            record_video_dir=str(OUT),
            record_video_size=VIEWPORT,
        )
        page = ctx.new_page()

        # 1. Login ------------------------------------------------------------
        page.goto(f"{BASE}/login")
        beat(page, 1200)
        page.locator("#username").press_sequentially("admin", delay=70)
        beat(page, 400)
        page.locator("#password").press_sequentially("AcceptPass456!", delay=70)
        beat(page, 500)
        page.get_by_role("button", name="Log in").click()
        page.wait_for_url("**/overview")

        # 2. Overview ---------------------------------------------------------
        page.wait_for_load_state("networkidle")
        beat(page, 4000)

        # 3. Models list ------------------------------------------------------
        page.locator(".ant-menu-item", has_text="Models").first.click()
        page.wait_for_load_state("networkidle")
        beat(page, 2500)

        # 4. Model detail: weighted routing -----------------------------------
        page.get_by_text("multi_version", exact=True).first.click()
        page.wait_for_load_state("networkidle")
        beat(page, 2500)
        page.get_by_role("button", name="Edit routing").click()
        beat(page, 1200)
        weights = page.locator(".ant-input-number input")
        weights.nth(0).fill("30")
        beat(page, 600)
        weights.nth(1).fill("70")
        beat(page, 900)
        page.get_by_role("button", name="Apply").first.click()
        beat(page, 1200)
        page.locator(".ant-modal .ant-btn-primary", has_text="Apply").click()
        page.wait_for_selector("text=Routing updated")
        beat(page, 2500)

        # 5. Access tab: grant a user ------------------------------------------
        page.locator(".ant-tabs-tab", has_text="Access").click()
        beat(page, 1500)
        page.locator("input[placeholder='username']").press_sequentially("alice", delay=90)
        beat(page, 600)
        page.get_by_role("button", name="Grant user").click()
        page.wait_for_selector("text=Access updated")
        beat(page, 2500)

        # 6. Playground: one predict call --------------------------------------
        page.locator(".ant-menu-item", has_text="Playground").click()
        page.wait_for_load_state("networkidle")
        beat(page, 1500)
        page.locator(".ant-card .ant-select").first.click()
        beat(page, 600)
        page.locator(
            ".ant-select-dropdown:not(.ant-select-dropdown-hidden) "
            ".ant-select-item-option",
            has_text="sentiment",
        ).click()
        beat(page, 800)
        body = page.locator("textarea").first
        body.fill("")
        body.press_sequentially(
            '{"input": "lite-server makes model serving delightful"}', delay=25
        )
        beat(page, 800)
        page.get_by_role("button", name="Send").click()
        page.wait_for_selector("text=Response", state="visible")
        beat(page, 3500)

        # 7. Back to Models: upload drawer --------------------------------------
        page.locator(".ant-menu-item", has_text="Models").first.click()
        beat(page, 2000)
        page.get_by_role("button", name="Upload model").click()
        page.wait_for_selector(".ant-drawer-open")
        beat(page, 3000)
        page.locator(".ant-drawer-close").click()
        beat(page, 1200)

        # 8. Delete-model confirm dialog (cancelled) ---------------------------
        echo_card = page.locator(".ant-card", has_text="echo").first
        echo_card.get_by_label("Actions").click()
        beat(page, 700)
        page.locator(".ant-dropdown-menu-item", has_text="Delete model").click()
        beat(page, 1000)
        page.locator(".ant-modal input[placeholder='echo']").press_sequentially(
            "echo", delay=110
        )
        beat(page, 1500)
        page.locator(".ant-modal .ant-btn", has_text="Cancel").click()
        beat(page, 1000)

        # 9. Metrics: live charts ----------------------------------------------
        page.locator(".ant-menu-item", has_text="Metrics").click()
        page.wait_for_load_state("networkidle")
        beat(page, 6000)

        ctx.close()
        browser.close()

    videos = sorted(OUT.glob("*.webm"), key=lambda f: f.stat().st_mtime)
    print(videos[-1] if videos else "no video produced", file=sys.stderr)


if __name__ == "__main__":
    main()
