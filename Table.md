
---

### **Connect to PostgreSQL and List Tables**

Check all tables:

```bash
psql -h localhost -U postgres -d networkdb -c "\dt"
```
Password: `postgres`

---

### **View Table Data**

**1. View `credential_profile` table:**

```bash
PGPASSWORD=postgres psql -h localhost -U postgres -d networkdb -c "\x" -c "SELECT * FROM credential_profile;"
```

**2. View `discovery_profile` table:**

```bash
PGPASSWORD=postgres psql -h localhost -U postgres -d networkdb -c "\x" -c "SELECT * FROM discovery_profile;"
```

**3. View `provision` table:**

```bash
PGPASSWORD=postgres psql -h localhost -U postgres -d networkdb -c "\x" -c "SELECT * FROM provision;"
```

---

✅ **Notes:**
- `\x` enables **expanded display** in `psql` (makes output neater when there are many columns).
- Using `PGPASSWORD=postgres` in front avoids password prompt (useful for quick testing).
- Table names you corrected are singular (`credential_profile`, `discovery_profile`, `provision`), not plural — **good catch!**

---




