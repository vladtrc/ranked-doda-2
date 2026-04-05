function playersTable(players, initialSortBy, initialSortDir) {
  return {
    players,
    sortBy: initialSortBy,
    sortDir: initialSortDir,
    sortedPlayers() {
      const dir = this.sortDir === 'asc' ? 1 : -1;
      return [...this.players].sort((a, b) => {
        const av = a[this.sortBy];
        const bv = b[this.sortBy];

        if (av === bv) {
          return a.name.localeCompare(b.name);
        }
        if (av == null) return 1;
        if (bv == null) return -1;
        if (typeof av === "string" && typeof bv === "string") {
          return av.localeCompare(bv) * dir;
        }
        return (av > bv ? 1 : -1) * dir;
      });
    },
    sort(col) {
      if (this.sortBy === col) {
        this.sortDir = this.sortDir === "asc" ? "desc" : "asc";
      } else {
        this.sortBy = col;
        this.sortDir = "desc";
      }
    }
  };
}
