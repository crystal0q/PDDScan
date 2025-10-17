@echo off
echo ============================================
echo 🚀 正在打包 PDDScan.exe ...
echo ============================================

REM 激活虚拟环境（如果有）
call .\.venv\Scripts\activate

REM 清理旧文件
rmdir /s /q build dist
del *.spec

REM 执行打包命令
pyinstaller -F -n PDDScan --add-data "price.xlsx;." --collect-all pandas --collect-all numpy --collect-all openpyxl --paths ".\.venv\Lib\site-packages" --clean --log-level=DEBUG main.py

echo ============================================
echo ✅ 打包完成！
echo 文件已生成：dist\PDDScan.exe
echo ============================================

pause
