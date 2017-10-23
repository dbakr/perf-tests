class PaddedMutableLong {
    public volatile long p1, p2, p3, p4, p5, p6 = 7L;
    private long value;

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public void add(long value) {
        this.value += value;
    }

    public long incrementAndGet() {
        return ++this.value;
    }

    public long getAndIncrement() {
        return this.value++;
    }
}
