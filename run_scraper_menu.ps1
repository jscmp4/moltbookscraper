$ErrorActionPreference = "Stop"

Set-Location -LiteralPath $PSScriptRoot

function Get-Runner {
    $conda = Get-Command conda -ErrorAction SilentlyContinue
    if ($conda) {
        return @{
            Kind = "conda"
            Cmd  = "conda"
            Base = @("run", "--no-capture-output", "-n", "base", "python", "-X", "utf8", "scraper.py")
        }
    }
    return @{
        Kind = "python"
        Cmd  = "python"
        Base = @("-X", "utf8", "scraper.py")
    }
}

function Invoke-Scraper {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$ArgsList
    )

    $runner = Get-Runner
    $cmd = @($runner.Base + $ArgsList)
    Write-Host ""
    Write-Host "[run] $($runner.Cmd) $($cmd -join ' ')" -ForegroundColor Cyan
    Write-Host ""
    & $runner.Cmd @cmd
}

function Ask-Int {
    param(
        [string]$Prompt,
        [int]$Default,
        [int]$Min = 0
    )
    while ($true) {
        $raw = Read-Host "$Prompt [$Default]"
        if ([string]::IsNullOrWhiteSpace($raw)) {
            return $Default
        }
        $n = 0
        if ([int]::TryParse($raw, [ref]$n) -and $n -ge $Min) {
            return $n
        }
        Write-Host "Please input an integer >= $Min" -ForegroundColor Yellow
    }
}

function Ask-YesNo {
    param(
        [string]$Prompt,
        [bool]$Default = $true
    )
    $def = if ($Default) { "Y/n" } else { "y/N" }
    while ($true) {
        $raw = Read-Host "$Prompt [$def]"
        if ([string]::IsNullOrWhiteSpace($raw)) {
            return $Default
        }
        switch -Regex ($raw.Trim().ToLowerInvariant()) {
            "^(y|yes|1)$" { return $true }
            "^(n|no|0)$"  { return $false }
            default { Write-Host "Please input y or n" -ForegroundColor Yellow }
        }
    }
}

function Ask-QueueStrategy {
    param(
        [string]$Default = "layered"
    )
    $valid = @("layered", "small-first", "large-first")
    while ($true) {
        $raw = Read-Host "comment-queue-strategy [layered/small-first/large-first] [$Default]"
        if ([string]::IsNullOrWhiteSpace($raw)) {
            return $Default
        }
        $v = $raw.Trim().ToLowerInvariant()
        if ($valid -contains $v) {
            return $v
        }
        Write-Host "Please input one of: layered, small-first, large-first" -ForegroundColor Yellow
    }
}

function Ask-CommentIdCache {
    param(
        [string]$Default = "memory"
    )
    $valid = @("memory", "sqlite")
    while ($true) {
        $raw = Read-Host "comment-id-cache [memory/sqlite] [$Default]"
        if ([string]::IsNullOrWhiteSpace($raw)) {
            return $Default
        }
        $v = $raw.Trim().ToLowerInvariant()
        if ($valid -contains $v) {
            return $v
        }
        Write-Host "Please input one of: memory, sqlite" -ForegroundColor Yellow
    }
}

function Run-Custom {
    $workers = Ask-Int "workers" 1 1
    $readRpm = Ask-Int "read-rpm" 40 1
    $commentRpm = Ask-Int "comment-rpm" 38 1
    $minComments = Ask-Int "min-comments" 30 0
    $maxCommentPosts = Ask-Int "max-comment-posts (0 = unlimited)" 0 0
    $queueStrategy = Ask-QueueStrategy "layered"
    $commentIdCache = Ask-CommentIdCache "memory"
    $queueSmallMax = 80
    $queueMediumMax = 400
    if ($queueStrategy -eq "layered") {
        $queueSmallMax = Ask-Int "queue-small-max (small <= N)" 80 1
        $queueMediumMax = Ask-Int "queue-medium-max (medium <= N, must > small)" 400 ($queueSmallMax + 1)
    }
    $useNoSnapshot = Ask-YesNo "disable auto snapshot (--no-snapshot)" $true
    $useFull = Ask-YesNo "run full mode (--full)" $false
    $useCommentsOnly = Ask-YesNo "comments-only mode (--comments-only)" $false
    $useNoResume = Ask-YesNo "ignore saved resume cursor (--no-resume)" $false
    $skipComments = Ask-YesNo "skip comments (--no-comments)" $false

    if ($useCommentsOnly -and $skipComments) {
        Write-Host "Invalid: --comments-only conflicts with --no-comments" -ForegroundColor Yellow
        return
    }

    $args = @("--workers", "$workers", "--read-rpm", "$readRpm", "--comment-rpm", "$commentRpm", "--min-comments", "$minComments", "--comment-queue-strategy", "$queueStrategy", "--comment-id-cache", "$commentIdCache")
    if ($maxCommentPosts -gt 0) {
        $args += @("--max-comment-posts", "$maxCommentPosts")
    }
    if ($queueStrategy -eq "layered") {
        $args += @("--queue-small-max", "$queueSmallMax", "--queue-medium-max", "$queueMediumMax")
    }
    if ($useNoSnapshot) { $args += "--no-snapshot" }
    if ($useFull) { $args += "--full" }
    if ($useCommentsOnly) { $args += "--comments-only" }
    if ($useNoResume) { $args += "--no-resume" }
    if ($skipComments) { $args += "--no-comments" }

    Invoke-Scraper -ArgsList $args
}

function Run-CommentsGapBackfill {
    $minComments = Ask-Int "min-comments threshold (comment_count >= N)" 30 0
    $maxCommentPosts = Ask-Int "max-comment-posts this run (0 = unlimited)" 0 0
    $workers = Ask-Int "workers" 1 1
    $commentRpm = Ask-Int "comment-rpm" 38 1
    $queueStrategy = Ask-QueueStrategy "layered"
    $commentIdCache = Ask-CommentIdCache "sqlite"
    $queueSmallMax = 80
    $queueMediumMax = 400
    if ($queueStrategy -eq "layered") {
        $queueSmallMax = Ask-Int "queue-small-max (small <= N)" 80 1
        $queueMediumMax = Ask-Int "queue-medium-max (medium <= N, must > small)" 400 ($queueSmallMax + 1)
    }
    $useNoResume = Ask-YesNo "ignore saved resume cursor (--no-resume)" $true

    $args = @("--workers", "$workers", "--comment-rpm", "$commentRpm", "--min-comments", "$minComments", "--comments-only", "--no-snapshot", "--comment-queue-strategy", "$queueStrategy", "--comment-id-cache", "$commentIdCache")
    if ($maxCommentPosts -gt 0) {
        $args += @("--max-comment-posts", "$maxCommentPosts")
    }
    if ($queueStrategy -eq "layered") {
        $args += @("--queue-small-max", "$queueSmallMax", "--queue-medium-max", "$queueMediumMax")
    }
    if ($useNoResume) { $args += "--no-resume" }

    Invoke-Scraper -ArgsList $args
}

function Read-MenuInput {
    try {
        $k = [Console]::ReadKey($true)
        return @{
            KeyName = $k.Key.ToString()
            KeyChar = "$($k.KeyChar)"
        }
    } catch {
        $raw = Read-Host "Input 1-9 / up/down/left/right / Enter / q"
        if ([string]::IsNullOrWhiteSpace($raw)) {
            return @{
                KeyName = "Enter"
                KeyChar = ""
            }
        }

        $txt = $raw.Trim().ToLowerInvariant()
        switch -Regex ($txt) {
            "^(q|quit|exit)$" {
                return @{
                    KeyName = "Q"
                    KeyChar = "q"
                }
            }
            "^(up|w)$" {
                return @{
                    KeyName = "UpArrow"
                    KeyChar = "w"
                }
            }
            "^(down|s)$" {
                return @{
                    KeyName = "DownArrow"
                    KeyChar = "s"
                }
            }
            "^(left|a)$" {
                return @{
                    KeyName = "LeftArrow"
                    KeyChar = "a"
                }
            }
            "^(right|d)$" {
                return @{
                    KeyName = "RightArrow"
                    KeyChar = "d"
                }
            }
            default {
                return @{
                    KeyName = "Unknown"
                    KeyChar = $txt.Substring(0, 1)
                }
            }
        }
    }
}

function Wait-AnyKeyOrEnter {
    param([string]$Prompt = "Press any key to continue...")
    Write-Host $Prompt -ForegroundColor DarkGray
    try {
        [void][Console]::ReadKey($true)
    } catch {
        [void](Read-Host "Press Enter to continue")
    }
}

function Select-MenuItem {
    param([array]$Items)

    $idx = 0
    while ($true) {
        Clear-Host
        Write-Host "Moltbook Scraper Menu" -ForegroundColor Cyan
        Write-Host "Use Up/Down/Left/Right, number 1-9, Enter to run, Q to quit." -ForegroundColor DarkGray
        Write-Host ""

        for ($i = 0; $i -lt $Items.Count; $i++) {
            $n = $i + 1
            if ($i -eq $idx) {
                Write-Host ("> [{0}] {1}" -f $n, $Items[$i].Name) -ForegroundColor Green
            } else {
                Write-Host ("  [{0}] {1}" -f $n, $Items[$i].Name)
            }
            Write-Host ("      {0}" -f $Items[$i].Desc) -ForegroundColor DarkGray
        }

        $input = Read-MenuInput
        $keyName = "$($input.KeyName)"
        $keyChar = "$($input.KeyChar)"

        switch ($keyName) {
            "UpArrow" {
                if ($idx -gt 0) { $idx-- }
                continue
            }
            "LeftArrow" {
                if ($idx -gt 0) { $idx-- }
                continue
            }
            "DownArrow" {
                if ($idx -lt $Items.Count - 1) { $idx++ }
                continue
            }
            "RightArrow" {
                if ($idx -lt $Items.Count - 1) { $idx++ }
                continue
            }
            "Enter" { return $idx }
            "Escape" { return -1 }
            "Q" { return -1 }
            default {
                if ($keyChar -match "^[1-9]$") {
                    $pick = [int]::Parse($keyChar) - 1
                    if ($pick -ge 0 -and $pick -lt $Items.Count) {
                        return $pick
                    }
                }
            }
        }
    }
}

$menu = @(
    @{
        Name = "Incremental (recommended)"
        Desc = "posts incremental + comments gap-backfill (>=30), layered queue, 1 worker"
        Run  = { Invoke-Scraper -ArgsList @("--workers", "1", "--min-comments", "30", "--comment-queue-strategy", "layered", "--comment-id-cache", "sqlite", "--no-snapshot") }
    },
    @{
        Name = "Only Posts Incremental"
        Desc = "fetch posts only, skip comments, no snapshot"
        Run  = { Invoke-Scraper -ArgsList @("--no-comments", "--no-snapshot") }
    },
    @{
        Name = "Comments Gap Backfill"
        Desc = "comments-only gap-backfill (skip posts); supports layered queue"
        Run  = { Run-CommentsGapBackfill }
    },
    @{
        Name = "Data Check"
        Desc = "run data health check with configurable eligible threshold"
        Run  = {
            $minComments = Ask-Int "check min-comments threshold (comment_count >= N)" 30 0
            $fastCheck = Ask-YesNo "fast check mode (skip strict comment audit, use cached comment metrics)" $false
            $args = @("--check", "--min-comments", "$minComments")
            if ($fastCheck) {
                $args += "--check-fast"
                $samplePosts = Ask-Int "fast check sample-posts for strict audit (0 = disable)" 300 0
                if ($samplePosts -gt 0) {
                    $args += @("--check-sample-posts", "$samplePosts")
                }
            }
            Invoke-Scraper -ArgsList $args
        }
    },
    @{
        Name = "Dedup Comments"
        Desc = "deduplicate comments_all.jsonl by comment id"
        Run  = { Invoke-Scraper -ArgsList @("--dedup-comments") }
    },
    @{
        Name = "Refetch High-Comment Posts"
        Desc = "force retry comments for posts >= N (reset done/resume/cooldown state)"
        Run  = {
            $n = Ask-Int "refetch threshold N (comment_count >= N)" 30 1
            Invoke-Scraper -ArgsList @("--refetch-comments", "$n")
        }
    },
    @{
        Name = "Snapshot Only"
        Desc = "collect hot/rising/top snapshot once"
        Run  = { Invoke-Scraper -ArgsList @("--snapshot") }
    },
    @{
        Name = "Clean runs/"
        Desc = "delete run snapshots; choose keep-last count"
        Run  = {
            $keep = Ask-Int "keep last N run files (0 = delete all)" 5 0
            Invoke-Scraper -ArgsList @("--clean-runs", "$keep")
        }
    },
    @{
        Name = "Custom Run"
        Desc = "interactive custom arguments"
        Run  = { Run-Custom }
    }
)

while ($true) {
    $pick = Select-MenuItem -Items $menu
    if ($pick -lt 0) { break }

    Clear-Host
    Write-Host ("Selected: {0}" -f $menu[$pick].Name) -ForegroundColor Cyan
    & $menu[$pick].Run

    Write-Host ""
    Wait-AnyKeyOrEnter -Prompt "Press any key to return menu..."
}
