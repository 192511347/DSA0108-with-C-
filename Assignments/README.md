# Object-Oriented Programming with C++ — Assignments and Code Debugging

**Student:** Shanmuganathan S R S  
**Register Number:** 192511347  
**Course:** Object-Oriented Programming with C++

## Repository structure

```text
.
├── Assignments/
│   ├── Assignment-1/
│   ├── Assignment-2/
│   ├── Assignment-3/
│   ├── Assignment-4/
│   └── Assignment-5/
├── Code-and-Debug/
│   ├── Unit-1/
│   ├── Unit-2/
│   ├── Unit-3/
│   ├── Unit-4/
│   └── Unit-5/
├── .gitignore
└── README.md
```

Each folder retains its original task structure, C++ sources, documentation, output screenshots, and supporting files. Compiled `.exe` files are excluded because they can be rebuilt from source.

## Run C++ code

For a standalone source file, use a C++ compiler such as g++:

```powershell
g++ -std=c++17 "path/to/program.cpp" -o program.exe
.\program.exe
```

Some tasks use multiple source/header files or require task-specific inputs; see their accompanying notes.

## Upload to GitHub

**Do not upload this ZIP directly as a repository file.** Extract the ZIP, open PowerShell in the extracted directory, and run:

```powershell
git init
git add .
git commit -m "Add OOP C++ assignments and code debugging"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPOSITORY.git
git push -u origin main
```

Create an **empty** repository on GitHub first and replace the example repository URL above with yours. If the repository already has commits, clone it first and copy these folders into that clone rather than force-pushing.

> Capstone materials are intentionally not included.
