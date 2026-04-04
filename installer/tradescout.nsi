; TradeScout Installer
; Built with NSIS 3.x + MUI2
; Installs TradeScout into <TAT_DIR>\TradeScout\ and optionally
; creates a Windows Scheduled Task for automatic daily execution.

!define PRODUCT_NAME    "TradeScout"
!define PRODUCT_PUBLISHER "TradeScout"
!define PRODUCT_URL     "https://github.com/breyer/TradeScout"
!define TASK_NAME       "TradeScout Daily Report"
!define TASK_MARKER     ".task_created"
!define CONFIG_FILE     "config\config.yaml"
!define CONFIG_DEMO     "config\config.demo.yaml"

; Version is injected at compile time: makensis /DVERSION=v2026.04.04-abc1234
!ifndef VERSION
  !define VERSION "dev"
!endif

; No admin rights needed — installs to user-local AppData
RequestExecutionLevel user

Name "${PRODUCT_NAME} ${VERSION}"
OutFile "..\TradeScout-Setup.exe"
InstallDir "$LOCALAPPDATA\TAT\TradeScout"
InstallDirRegKey HKCU "Software\${PRODUCT_NAME}" "InstallDir"
SetCompressor /SOLID lzma

; ── MUI2 ────────────────────────────────────────────────────────────────────
!include "MUI2.nsh"
!include "nsDialogs.nsh"
!include "LogicLib.nsh"

!define MUI_ABORTWARNING
!define MUI_ICON    "${NSISDIR}\Contrib\Graphics\Icons\modern-install.ico"
!define MUI_UNICON  "${NSISDIR}\Contrib\Graphics\Icons\modern-uninstall.ico"

; Pages
!insertmacro MUI_PAGE_WELCOME
!insertmacro MUI_PAGE_LICENSE "license.txt"
Page custom PageTATDir PageTATDirLeave
Page custom PageWebhook PageWebhookLeave
Page custom PageFeatures PageFeaturesLeave
!insertmacro MUI_PAGE_INSTFILES
!define MUI_FINISHPAGE_RUN         "$INSTDIR\TradeScout.exe"
!define MUI_FINISHPAGE_RUN_TEXT    "Launch TradeScout now"
!define MUI_FINISHPAGE_SHOWREADME  "$INSTDIR\${CONFIG_FILE}"
!define MUI_FINISHPAGE_SHOWREADME_TEXT "Open config.yaml"
!insertmacro MUI_PAGE_FINISH

!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES

!insertmacro MUI_LANGUAGE "English"

; ── Variables ────────────────────────────────────────────────────────────────
Var TATDir
Var TATDirCtrl
Var WebhookURL
Var WebhookCtrl
Var IsUpgrade          ; "1" if config.yaml already exists
Var EquityCurve        ; "1" = enabled
Var RollingBenchmarks  ; "1" = enabled

; ── .onInit — detect TAT directory ──────────────────────────────────────────
Function .onInit
  StrCpy $TATDir ""

  ; 1. Registry probe
  ReadRegStr $0 HKCU "Software\Trade Automation Toolbox" "InstallPath"
  ${If} $0 != ""
  ${AndIf} ${FileExists} "$0\TradeAutomationToolbox.exe"
    StrCpy $TATDir $0
    Goto FoundTAT
  ${EndIf}
  ReadRegStr $0 HKCU "Software\Trade Automation Toolbox" "InstallPath"
  ${If} $0 != ""
  ${AndIf} ${FileExists} "$0\TAT.exe"
    StrCpy $TATDir $0
    Goto FoundTAT
  ${EndIf}

  ; 2. Known default paths — validate by checking for a TAT executable
  ${If} ${FileExists} "$LOCALAPPDATA\TAT\TradeAutomationToolbox.exe"
    StrCpy $TATDir "$LOCALAPPDATA\TAT"
    Goto FoundTAT
  ${EndIf}
  ${If} ${FileExists} "$LOCALAPPDATA\TAT\TAT.exe"
    StrCpy $TATDir "$LOCALAPPDATA\TAT"
    Goto FoundTAT
  ${EndIf}
  ${If} ${FileExists} "$LOCALAPPDATA\Trade Automation Toolbox\TradeAutomationToolbox.exe"
    StrCpy $TATDir "$LOCALAPPDATA\Trade Automation Toolbox"
    Goto FoundTAT
  ${EndIf}
  ${If} ${FileExists} "$PROGRAMFILES\TAT\TradeAutomationToolbox.exe"
    StrCpy $TATDir "$PROGRAMFILES\TAT"
    Goto FoundTAT
  ${EndIf}

  ; 3. Fallback — use default, let the user browse
  StrCpy $TATDir "$LOCALAPPDATA\TAT"

  FoundTAT:
  StrCpy $INSTDIR "$TATDir\TradeScout"

  ; Check for existing install (upgrade detection)
  ${If} ${FileExists} "$INSTDIR\${CONFIG_FILE}"
    StrCpy $IsUpgrade "1"
  ${Else}
    StrCpy $IsUpgrade "0"
  ${EndIf}

  ; Feature defaults
  StrCpy $EquityCurve       "1"
  StrCpy $RollingBenchmarks "1"
FunctionEnd

; ── Custom Page: TAT Directory ───────────────────────────────────────────────
Function PageTATDir
  !insertmacro MUI_HEADER_TEXT "TAT Install Directory" \
    "TradeScout will be installed inside your TAT folder."

  nsDialogs::Create 1018
  Pop $0

  ${NSD_CreateLabel} 0 0 100% 24u \
    "TradeScout detected the following TAT directory. Change it if needed, \
then click Next."
  Pop $0

  ${NSD_CreateDirRequest} 0 30u 80% 14u $TATDir
  Pop $TATDirCtrl

  ${NSD_CreateBrowseButton} 82% 29u 18% 15u "Browse..."
  Pop $0
  nsDialogs::OnClick $0 PageTATDirBrowse

  ${NSD_CreateLabel} 0 52u 100% 20u \
    "TradeScout will be installed into: <TAT DIR>\TradeScout\"
  Pop $0

  nsDialogs::Show
FunctionEnd

Function PageTATDirBrowse
  nsDialogs::SelectFolderDialog "Select your TAT installation folder" $TATDir
  Pop $0
  ${If} $0 != "error"
    ${NSD_SetText} $TATDirCtrl $0
  ${EndIf}
FunctionEnd

Function PageTATDirLeave
  ${NSD_GetText} $TATDirCtrl $TATDir
  ${If} $TATDir == ""
    MessageBox MB_OK|MB_ICONEXCLAMATION "Please select a TAT directory."
    Abort
  ${EndIf}
  StrCpy $INSTDIR "$TATDir\TradeScout"

  ; Warn (don't block) if TAT executable not found in the chosen directory
  ${IfNot} ${FileExists} "$TATDir\TradeAutomationToolbox.exe"
  ${AndIfNot} ${FileExists} "$TATDir\TAT.exe"
    MessageBox MB_YESNO|MB_ICONEXCLAMATION \
      "No TAT executable was found in:$\r$\n$TATDir$\r$\n$\r$\n\
TradeScout reads the TAT database from this location. \
Are you sure this is the correct folder?" \
      IDYES ContinueTAT
    Abort
    ContinueTAT:
  ${EndIf}

  ; Refresh upgrade flag for the chosen directory
  ${If} ${FileExists} "$INSTDIR\${CONFIG_FILE}"
    StrCpy $IsUpgrade "1"
  ${Else}
    StrCpy $IsUpgrade "0"
  ${EndIf}
FunctionEnd

; ── Custom Page: Discord Webhook ─────────────────────────────────────────────
Function PageWebhook
  ; Skip this page on upgrade — existing config is preserved
  ${If} $IsUpgrade == "1"
    Abort
  ${EndIf}

  !insertmacro MUI_HEADER_TEXT "Discord Webhook" \
    "Enter your Discord webhook URL to receive daily reports."

  nsDialogs::Create 1018
  Pop $0

  ${NSD_CreateLabel} 0 0 100% 24u \
    "Paste your Discord webhook URL below. You can leave this blank and \
add it to config\config.yaml after installation."
  Pop $0

  ${NSD_CreateText} 0 30u 100% 14u "https://discord.com/api/webhooks/..."
  Pop $WebhookCtrl

  ${NSD_CreateLabel} 0 52u 100% 30u \
    "To create a webhook: open Discord $\"Server Settings$\" $\u2192 \
$\"Integrations$\" $\u2192 $\"Webhooks$\" $\u2192 $\"New Webhook$\"."
  Pop $0

  nsDialogs::Show
FunctionEnd

Function PageWebhookLeave
  ${NSD_GetText} $WebhookCtrl $WebhookURL

  ; Treat the placeholder text as blank
  ${If} $WebhookURL == "https://discord.com/api/webhooks/..."
    StrCpy $WebhookURL ""
  ${EndIf}

  ; Validate non-empty input
  ${If} $WebhookURL != ""
    StrLen $0 $WebhookURL
    ${If} $0 < 30
      MessageBox MB_OK|MB_ICONEXCLAMATION \
        "The URL looks too short. A Discord webhook URL starts with:$\r$\n\
https://discord.com/api/webhooks/"
      Abort
    ${EndIf}
  ${EndIf}
FunctionEnd

; ── Custom Page: Optional Features ───────────────────────────────────────────
Var ChkEquity
Var ChkRolling

Function PageFeatures
  ; Skip on upgrade
  ${If} $IsUpgrade == "1"
    Abort
  ${EndIf}

  !insertmacro MUI_HEADER_TEXT "Optional Features" \
    "Choose which features to enable in your daily report."

  nsDialogs::Create 1018
  Pop $0

  ${NSD_CreateLabel} 0 0 100% 20u \
    "Both features are enabled by default. You can change them later in \
config\config.yaml."
  Pop $0

  ${NSD_CreateCheckbox} 0 28u 100% 14u \
    "Equity Curve — attach a cumulative P&&L chart image to each report"
  Pop $ChkEquity
  ${NSD_SetState} $ChkEquity ${BST_CHECKED}

  ${NSD_CreateCheckbox} 0 48u 100% 14u \
    "Rolling Benchmarks — show 5 / 20 / 60-day rolling stats in each report"
  Pop $ChkRolling
  ${NSD_SetState} $ChkRolling ${BST_CHECKED}

  nsDialogs::Show
FunctionEnd

Function PageFeaturesLeave
  ${NSD_GetState} $ChkEquity  $0
  ${If} $0 == ${BST_CHECKED}
    StrCpy $EquityCurve "1"
  ${Else}
    StrCpy $EquityCurve "0"
  ${EndIf}

  ${NSD_GetState} $ChkRolling $0
  ${If} $0 == ${BST_CHECKED}
    StrCpy $RollingBenchmarks "1"
  ${Else}
    StrCpy $RollingBenchmarks "0"
  ${EndIf}
FunctionEnd

; ── Helper: bool string → yaml value ─────────────────────────────────────────
!macro BoolToYaml VAR OUT
  ${If} ${VAR} == "1"
    StrCpy ${OUT} "true"
  ${Else}
    StrCpy ${OUT} "false"
  ${EndIf}
!macroend

; ── Install Section ───────────────────────────────────────────────────────────
Section "TradeScout" SecMain
  SectionIn RO  ; mandatory

  SetOutPath "$INSTDIR"
  File "..\dist\TradeScout.exe"

  ; Config directory
  SetOutPath "$INSTDIR\config"
  File /oname=config.demo.yaml "..\config\config.demo.yaml"

  ; Write config.yaml only on fresh install
  ${If} $IsUpgrade == "0"
    Call WriteConfig
  ${EndIf}

  ; Uninstaller
  WriteUninstaller "$INSTDIR\Uninstall.exe"

  ; Start Menu
  CreateDirectory "$SMPROGRAMS\${PRODUCT_NAME}"
  CreateShortCut "$SMPROGRAMS\${PRODUCT_NAME}\${PRODUCT_NAME}.lnk" \
    "$INSTDIR\TradeScout.exe" "" "$INSTDIR\TradeScout.exe" 0
  CreateShortCut "$SMPROGRAMS\${PRODUCT_NAME}\Uninstall.lnk" \
    "$INSTDIR\Uninstall.exe"

  ; Add/Remove Programs entry
  WriteRegStr HKCU \
    "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}" \
    "DisplayName" "${PRODUCT_NAME} ${VERSION}"
  WriteRegStr HKCU \
    "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}" \
    "UninstallString" "$INSTDIR\Uninstall.exe"
  WriteRegStr HKCU \
    "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}" \
    "InstallLocation" "$INSTDIR"
  WriteRegStr HKCU \
    "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}" \
    "DisplayVersion" "${VERSION}"
  WriteRegStr HKCU \
    "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}" \
    "Publisher" "${PRODUCT_PUBLISHER}"
  WriteRegStr HKCU \
    "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}" \
    "URLInfoAbout" "${PRODUCT_URL}"
  WriteRegDWORD HKCU \
    "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}" \
    "NoModify" 1
  WriteRegDWORD HKCU \
    "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}" \
    "NoRepair" 1

  ; Store install dir for future upgrades
  WriteRegStr HKCU "Software\${PRODUCT_NAME}" "InstallDir" "$INSTDIR"

  ; Scheduled Task
  MessageBox MB_YESNO|MB_ICONQUESTION \
    "Create a Windows Scheduled Task to run TradeScout automatically at \
4:15 PM (Eastern Time) on weekdays?$\r$\n$\r$\nYou can remove it later \
from Windows Task Scheduler or by uninstalling TradeScout." \
    IDNO SkipTask

    nsExec::ExecToLog \
      'schtasks /Create /TN "${TASK_NAME}" \
/TR "\"$INSTDIR\TradeScout.exe\"" \
/SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 16:15 /F'
    FileOpen $0 "$INSTDIR\${TASK_MARKER}" w
    FileClose $0

  SkipTask:
SectionEnd

; ── Write config.yaml ────────────────────────────────────────────────────────
Function WriteConfig
  ; Build feature flag strings
  Var /GLOBAL EquityCurveYaml
  Var /GLOBAL RollingYaml

  !insertmacro BoolToYaml $EquityCurve $EquityCurveYaml
  !insertmacro BoolToYaml $RollingBenchmarks $RollingYaml

  ; Determine db_path — TAT typically stores its DB under data\data.db3
  ; Use forward slashes as required by config.yaml
  StrCpy $1 "$TATDir\data\data.db3"
  ; Convert backslashes to forward slashes
  ${WordReplace} $1 "\" "/" "+" $2
  ; $2 now has forward-slash path

  ; Determine webhook section
  StrCpy $3 ""
  ${If} $WebhookURL != ""
    StrCpy $3 '  - url: "$WebhookURL"'
  ${Else}
    StrCpy $3 '  - url: "https://discord.com/api/webhooks/YOUR_ID/YOUR_TOKEN"'
  ${EndIf}

  FileOpen $0 "$INSTDIR\${CONFIG_FILE}" w
  FileWrite $0 "# config.yaml — TradeScout configuration$\r$\n"
  FileWrite $0 "# Generated by the installer. Edit this file to adjust settings.$\r$\n"
  FileWrite $0 "$\r$\n"
  FileWrite $0 "# Path to your TAT database (use forward slashes)$\r$\n"
  FileWrite $0 'db_path: "$2"$\r$\n'
  FileWrite $0 "$\r$\n"
  FileWrite $0 "webhooks:$\r$\n"
  FileWrite $0 "  # Add your Discord webhook URL(s) here.$\r$\n"
  FileWrite $0 "  # Get one via: Discord Server Settings > Integrations > Webhooks.$\r$\n"
  FileWrite $0 "$3$\r$\n"
  FileWrite $0 "$\r$\n"
  FileWrite $0 "features:$\r$\n"
  FileWrite $0 "$\r$\n"
  FileWrite $0 "  # Attach a cumulative P&L equity curve chart to each report.$\r$\n"
  FileWrite $0 "  equity_curve:$\r$\n"
  FileWrite $0 "    enabled: $EquityCurveYaml$\r$\n"
  FileWrite $0 "    days: 60$\r$\n"
  FileWrite $0 "$\r$\n"
  FileWrite $0 "  # Append rolling 5/20/60-day benchmarks to each report.$\r$\n"
  FileWrite $0 "  rolling_benchmarks:$\r$\n"
  FileWrite $0 "    enabled: $RollingYaml$\r$\n"
  FileWrite $0 "    windows: [5, 20, 60]$\r$\n"
  FileWrite $0 "$\r$\n"
  FileWrite $0 "  # Skip posting when the market was closed (no trades + no DailyLog data).$\r$\n"
  FileWrite $0 "  skip_closed_market:$\r$\n"
  FileWrite $0 "    enabled: true$\r$\n"
  FileClose $0
FunctionEnd

; ── Uninstaller ───────────────────────────────────────────────────────────────
Section "Uninstall"
  ; Remove scheduled task if it was created by this installer
  ${If} ${FileExists} "$INSTDIR\${TASK_MARKER}"
    nsExec::ExecToLog 'schtasks /Delete /TN "${TASK_NAME}" /F'
    Delete "$INSTDIR\${TASK_MARKER}"
  ${EndIf}

  ; Remove files (preserve config.yaml)
  Delete "$INSTDIR\TradeScout.exe"
  Delete "$INSTDIR\config\config.demo.yaml"
  Delete "$INSTDIR\Uninstall.exe"

  ; Offer to remove config.yaml
  ${If} ${FileExists} "$INSTDIR\${CONFIG_FILE}"
    MessageBox MB_YESNO|MB_ICONQUESTION \
      "Remove config.yaml (your webhook URL and settings)?$\r$\n\
Click No to keep it for a future reinstall." \
      IDNO KeepConfig
      Delete "$INSTDIR\${CONFIG_FILE}"
    KeepConfig:
  ${EndIf}

  ; Remove directories (only if empty)
  RMDir "$INSTDIR\config"
  RMDir "$INSTDIR"

  ; Start Menu
  Delete "$SMPROGRAMS\${PRODUCT_NAME}\${PRODUCT_NAME}.lnk"
  Delete "$SMPROGRAMS\${PRODUCT_NAME}\Uninstall.lnk"
  RMDir  "$SMPROGRAMS\${PRODUCT_NAME}"

  ; Registry
  DeleteRegKey HKCU \
    "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}"
  DeleteRegKey HKCU "Software\${PRODUCT_NAME}"
SectionEnd
