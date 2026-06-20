package com.kuaia.engine.coordinator.dispatch;

/** 重试上限策略常量(单一真值源,两条重试入口共用)。 */
public final class TaskRetryPolicy {
    /** 默认总尝试上限:1 次首发 + 3 次重试。 */
    public static final int DEFAULT_MAX_ATTEMPTS = 4;

    private TaskRetryPolicy() {
    }
}
