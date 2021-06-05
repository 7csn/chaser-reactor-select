<?php

namespace chaser\reactor;

use SplPriorityQueue;

/**
 * 常规事件反应类
 *
 * @package chaser\reactor
 */
class Select extends Reactor
{
    /**
     * 读写事件默认等待时间（微秒）
     */
    public const SELECT_TIMEOUT = 100000000;

    /**
     * 定时器优先级队列
     *
     * @var SplPriorityQueue
     */
    private SplPriorityQueue $scheduler;

    /**
     * 读写事件等待时间（微秒）
     *
     * @var int
     */
    private int $selectTimeout = self::SELECT_TIMEOUT;

    /**
     * 侦听读事件资源句柄集合
     *
     * @var resource[]
     */
    private array $readFds = [];

    /**
     * 侦听写事件资源句柄集合
     *
     * @var resource[]
     */
    private array $writeFds = [];

    /**
     * 退出回路
     *
     * @var bool
     */
    private bool $exit = false;

    /**
     * 初始化队列
     */
    public function __construct()
    {
        $this->initScheduler();
    }

    /**
     * @inheritDoc
     */
    public function loop(): void
    {
        $this->exit = false;

        while ($this->exit === false) {
            // 处理等待信号
            pcntl_signal_dispatch();

            $reads = $this->readFds;
            $writes = $this->writeFds;

            if ($reads || $writes) {
                // 等待流读写事件，被信号打断警告并返回 false
                $excepts = null;
                $fdEventCount = @stream_select($reads, $writes, $excepts, 0, $this->selectTimeout);
            } else {
                usleep($this->selectTimeout);
                $fdEventCount = false;
            }

            // 若有定时器，检测是否触发
            if (!$this->scheduler->isEmpty()) {
                $this->tick();
            }

            // 资源句柄读写回调
            if ($fdEventCount) {
                foreach ($reads as $read) {
                    $this->readCallback($read);
                }
                foreach ($writes as $write) {
                    $this->writeCallback($write);
                }
            }
        }
    }

    /**
     * @inheritDoc
     */
    public function destroy(): void
    {
        $this->exit = true;
    }

    /**
     * @inheritDoc
     */
    public function clearTimer(): void
    {
        $this->initScheduler();
        parent::clearTimer();
    }

    /**
     * @inheritDoc
     *
     * @return callable
     */
    protected function makeReadData(int $intFd, $fd, callable $callback): callable
    {
        $this->readFds[$intFd] = $fd;
//        $this->fds[self::EV_READ][$intFd] = $fd;
        return $callback;
    }

    /**
     * @inheritDoc
     *
     * @return callable
     */
    protected function makeWriteData(int $intFd, $fd, callable $callback): callable
    {
        $this->writeFds[$intFd] = $fd;
//        $this->fds[self::EV_WRITE][$intFd] = $fd;
        return $callback;
    }

    /**
     * @inheritDoc
     *
     * @return callable|false
     */
    protected function makeSignalData(int $signal, callable $callback): callable|false
    {
        return pcntl_signal($signal, fn($signal) => $this->signalCallback($signal)) ? $callback : false;
    }

    /**
     * @inheritDoc
     *
     * @return array
     */
    protected function makeIntervalData(int $timerId, int $seconds, callable $callback): array
    {
        return $this->addTimerData($timerId, self::EV_INTERVAL, $seconds, $callback);
    }

    /**
     * @inheritDoc
     *
     * @return array
     */
    protected function makeTimeoutData(int $timerId, int $seconds, callable $callback): array
    {
        return $this->addTimerData($timerId, self::EV_TIMEOUT, $seconds, $callback);
    }

    /**
     * @inheritDoc
     */
    protected function delDataModel(int $flag, int $key): bool
    {
        switch ($flag) {
            case self::EV_READ:
                unset($this->readFds[$key]);
                return true;
            case self::EV_WRITE:
                unset($this->writeFds[$key]);
                return true;
            case self::EV_SIGNAL:
                return pcntl_signal($key, SIG_IGN);
            case self::EV_INTERVAL:
            case self::EV_TIMEOUT:
                return true;
        }
        return false;
    }

    /**
     * 获取定时器事件添加数据
     *
     * @param int $timerId
     * @param int $flag
     * @param int $seconds
     * @param callable $callback
     * @return array
     */
    private function addTimerData(int $timerId, int $flag, int $seconds, callable $callback): array
    {
        // 计划执行时间
        $runtime = microtime(true) + $seconds;

        // 加入队列：定时器ID、优先级（计划时间负值，计划越远，优先级越低）
        $this->scheduler->insert([$timerId, $flag], -$runtime);

        // 设置资源句柄读写事件等待时间，防止闹铃超时
        $this->selectTimeout = min($this->selectTimeout, $seconds * 1000000);

        // 返回定时器数据
        return [$seconds, $callback];
    }

    /**
     * 闹钟走时：从前往后检测闹铃
     */
    private function tick(): void
    {
        do {
            // 查看最前闹铃
            $data = $this->scheduler->top();
            [$timerId, $flag] = $data['data'];
            $nextRuntime = -$data['priority'];

            // 计算剩余时间
            $now = microtime(true);
            $leftTime = $nextRuntime - $now;

            // 闹铃未响，则重置等待时间
            if ($leftTime > 0) {
                $this->selectTimeout = min($leftTime * 1000000, self::SELECT_TIMEOUT);
                return;
            }

            // 闹铃响起，清出队列；闹铃回调处理
            $this->scheduler->extract();
            $this->timerCallback($timerId, $flag, $now);

        } while (!$this->scheduler->isEmpty());

        // 无定时器，重置流事件等待时间
        $this->selectTimeout = self::SELECT_TIMEOUT;
    }

    /**
     * 流事件处理程序
     *
     * @param resource $fd
     */
    private function readCallback($fd): void
    {
        $this->events[self::EV_READ][(int)$fd]($fd);
    }

    /**
     * 流事件处理程序
     *
     * @param resource $fd
     */
    private function writeCallback($fd): void
    {
        $this->events[self::EV_WRITE][(int)$fd]($fd);
    }

    /**
     * 信号事件处理程序
     *
     * @param int $signal
     */
    private function signalCallback(int $signal): void
    {
        $this->events[self::EV_SIGNAL][$signal]($signal);
    }

    /**
     * 定时器事件处理程序
     *
     * @param int $timerId
     * @param int $flag
     * @param float $now
     */
    private function timerCallback(int $timerId, int $flag, float $now): void
    {
        if (isset($this->events[$flag][$timerId])) {

            // 定时器信息：间隔秒数、回调程序
            [$seconds, $callback] = $this->events[$flag][$timerId];

            // 持续性判断：单次，清除任务事件；持续，追加队列
            if ($flag === self::EV_TIMEOUT) {
                $this->delTimeout($timerId);
            } else {
                $nextRuntime = $now + $seconds;
                $this->scheduler->insert([$timerId, $flag], -$nextRuntime);
            }

            // 执行闹铃任务
            $callback($timerId);
        }
    }

    /**
     * 初始化定时器优先级队列
     */
    private function initScheduler(): void
    {
        $this->scheduler = new SplPriorityQueue();
        $this->scheduler->setExtractFlags(SplPriorityQueue::EXTR_BOTH);
    }
}
