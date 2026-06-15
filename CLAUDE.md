# Coding Standards & Conventions

## General Python
- Use functional, declarative programming — avoid classes where possible
- Prefer iteration and modularization over code duplication
- Use descriptive variable names with auxiliary verbs (e.g., `is_active`, `has_permission`)
- Use lowercase with underscores for directories and files (e.g., `routers/user_routes.py`)
- Favor named exports for routes and utility functions
- Use the Receive an Object, Return an Object (RORO) pattern
- Use `def` for pure/synchronous functions and `async def` for asynchronous operations
- Type hints required on all function signatures
- Prefer Pydantic `BaseModel` over raw dictionaries for input validation
- No emojis or symbols in any Python files

## Code Style
- Avoid unnecessary curly braces in conditional statements
- Omit curly braces for single-line conditionals
- Use concise one-line syntax for simple conditionals (e.g., `if condition: do_something()`)

## File Structure
Each module should follow this order:
1. Exported router
2. Sub-routes
3. Utilities
4. Static content
5. Types (models, schemas)

## FastAPI
- Use functional components (plain functions) and Pydantic models for validation and response schemas
- Use declarative route definitions with explicit return type annotations
- Prefer lifespan context managers over `@app.on_event("startup")` / `@app.on_event("shutdown")`
- Use middleware for logging, error monitoring, and performance optimization
- Use `HTTPException` for expected errors modeled as specific HTTP responses
- Use middleware for unexpected errors, logging, and error monitoring
- Refer to FastAPI docs for Data Models, Path Operations, and Middleware best practices

## Database & Async
- Use async functions for all I/O-bound tasks (database calls, external API requests)
- Minimize blocking I/O operations — async required for all DB and external requests
- Preferred async DB libraries: `asyncpg` or `aiomysql`
- Use SQLAlchemy 2.0 for ORM features when needed

## Performance
- Implement caching for static and frequently accessed data (Redis or in-memory)
- Use lazy loading for large datasets and substantial API responses
- Optimize data serialization/deserialization with Pydantic