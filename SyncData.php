<?php

/**
 * Created by PhpStorm.
 * User: vision
 * Date: 18.10.17
 * Time: 12:57
 */
class SyncData extends Base
{

    /**
     * @var App\Helpers\Cache
     */
    protected $cache;
    protected $offset = 3; //minutes
    protected $workers = [];
    protected $active = false;
    //Количество воркеров
    protected $countWorkers = 2;
    protected $setSignalHandler = false;
    protected $iAmWorker = false;
    public $pidFile = APP_DIR . DIRECTORY_SEPARATOR . 'sem' . DIRECTORY_SEPARATOR . 'pid_daemon.txt';


    protected function configure()
    {
        $this->setName('sync:data');
        $this->setDescription('Sync data');
        $this->cache = new App\Helpers\Cache();
        parent::configure();
    }


    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $this->active = true;
        if(!$this->checkMyCopy()){
            exit(0);
        }

        $this->startLoop();
        $this->afterDie();
    }


    /**
     * Основной цикл
     */
    protected function startLoop()
    {
        while (true) {
            //Проверяем файлик-семафор
            if ($this->checkSem('stop-sync-data.txt') || !$this->active) {
                $this->log('Sync data is stopped');
                exit(0);
            }

            try{
                // Дата последней успешной синхронизации
                $last_sync = dataModel::getLastUpdate();
                $lt = Func::getFromArray('end_time', $last_sync);
                $last_sync_time = new \DateTime($lt);

                // Если время не 6 утра, то не запускаем синхронизацию
                // Если время прошедшее с момента последней синхронизации меньше 10ч - так же не запускаем синхронизацию
                if (!$this->checkTime($last_sync_time)) {
                    $this->sleep();
                    continue;
                }

                //если в БД нет записей о последней синхр. то считаем что это текущее время минус 120 минут
                if(empty($lt)){
                    $last_sync_time->sub(new \DateInterval('PT' . ($this->offset+120) . 'M'));
                }
                //временем окончания синхронизации считаем текущее время минус 5 минут
                $endTimeSync = new \DateTime();
                $endTimeSync->sub(new \DateInterval('PT5M'));
                //разбиваем период времени которые будем синхронизировать на отрезки по $this->offset минут
                //и для каждого воркера формируем массив с его участоком времени
                //создаем форк на каждый участок времени
                foreach($this->timeChunk($last_sync_time, $endTimeSync) as $part) {
                    //создаем форк
                    $child_pid = pcntl_fork();
                    if ($child_pid == -1) {
                        die ("Can't fork process");
                    } elseif ($child_pid) {
                        //эта часть выполнится в основном потоке
                        $this->log("Parent, created child: $child_pid");
                        $this->workers[] = $child_pid;
                    } else {
                        $this->iAmWorker = true;
                        $this->work($part);
                        //на этом работа воркера завершена
                        exit(0);
                    }
                }

                //все дальнейшее выполнится только в головном процессе
                //устанавливаем обработчик сигналов для головного процесса
                $this->setSignalHandlers('signalMainHandler');
                //Переподключаемся к БД
                //так как воркеры убили подключение
                App\Models\BaseModel::recconect();
                //Ожидаем пока все воркеры отработают
                $this->waitWorkers();

            } catch(\Exception $e) {
                $this->log('Exception ' . $e->getMessage());
                if($this->iAmWorker) {
                    exit(0);
                }
            }
            $this->killWorkers();
            $this->sendNotification();
            $this->sleep();
        }
    }


    /**
     * Ожидаем пока все воркеры отработают
     */
    protected  function waitWorkers()
    {
        //Ожидаем завершения процессов
        while (($signaled_pid = pcntl_waitpid(-1, $status, WNOHANG)) || count($this->workers) > 0) {
            pcntl_signal_dispatch();
            if ($signaled_pid == -1) {
                //детей не осталось
                $this->workers = [];
                break;
            } elseif($signaled_pid) {
                $this->log('Worker ' . $signaled_pid . ' done');
                unset($this->workers[$signaled_pid]);
            }
        }
    }


    /**
     * Работа выполняемая воркером
     * @param array $part
     */
    protected function work(array $part)
    {
        /**
         * Для каждого воркера создаем свое подключение к БД и mc
         * Так как самый первый завершившийся воркер может навредить другим воркерам
         */
        $this->initMainPort();
        App\Models\BaseModel::recconect();
        //Вешаем обработчик сигналов
        //так мы сможем управлять потомками с головного процесса
        $this->setSignalHandlers('signalHandler');
        // Синкаем период времени для конкретного воркера
        foreach($part as $pTime){
            pcntl_signal_dispatch();
            try{
                if(!$this->sync($pTime['start_time'], $pTime['end_time'])){
                    $this->log('Error sync payment');
                    $this->addError('Error sync payment');
                }
            } catch(\Exception $e) {
                $this->addError('Worker errors: <br>' . $e->getMessage());
                $this->logFile($e->getMessage(), 'worker_errors.txt');
            }

        }
        $this->sendNotification();
        App\Models\BaseModel::closeConnect();
    }


    protected function sync(\DateTime $start, \DateTime $end, $last_payment_id = 0)
    {
        $this->log('Worker ' . getmypid() .  ' sync ' . $start->format('Y-m-d\TH:i:s') . " : " . $end->format('Y-m-d\TH:i:s'));
        if ($this->checkSem('stop-sync-data.txt') || !$this->checkTime() || !$this->active) {
            $this->log('Worker ' . getmypid() . ' closed.');
            exit(0);
        }

        $count = 0;
        $list = [];
        $totalAmount = 0;
        $response = $this->getdata($start, $end);

        if(!$response){
            $this->log('Error data ' . print_r($response, true));
            dataModel::createOrUpdate($start, $end, $count, dataModel::STATUS_CANCEL, 0, [print_r($response, 1)]);
            return false;
        }

        //если данных для синхронизации не пришло, то можем спокойно переходить к след итерации
        if(
            !is_object($response) ||
            !property_exists($response, 'GetHourdataResult') ||
            !property_exists($response->GetHourdataResult, 'PaymentFor1C')
        ){
            dataModel::createOrUpdate($start, $end, 0, dataModel::STATUS_SUCCESS, $last_payment_id);
            $this->log('Worker ' . getmypid() . ' got error response');
            return true;
        }

        foreach($this->objectToArray($response->GetHourdataResult->PaymentFor1C) as $payment){

            if(
                !is_object($payment)
                || !property_exists($payment, 'PointId')
                || !property_exists($payment, 'CardId')
            ){
                $this->logFile($payment, 'dataErr.txt');
                $this->addError('dataErr:' . print_r($payment,1));
                continue;
            }

            //проверяем разрешено ли синхронизацию для данной пары
            if (!App\Models\PointsModel::hasEnableSync($payment->PointId, $payment->CardId)) {
                continue;
            }

            $count++;
            $totalAmount += $payment->Cash;
            $list[] = $this->createPayObject($payment);
            $last_payment_id = $payment->PaymentId > $last_payment_id ? $payment->PaymentId : $last_payment_id;
        }

        if(empty($list)){
            dataModel::createOrUpdate($start, $end, 0, dataModel::STATUS_SUCCESS, $last_payment_id);
            $this->log('Worker ' . getmypid() . ' got 0 data');
            return true;
        }

        $data = [
            'ReportData' => [
                'ReportDate' => date('d.m.Y'),
                'TotalAmount' => $totalAmount * 100,
                'RowsCount' => $count,
                'ReportRows' => $list
            ]
        ];
        $this->log('Total payment rows, pid  ' . getmypid() . ', count: ' . $count);

        $resultSync = $this->sendToNC('PL_Report', $data, false);

        if(!$this->hasError($resultSync)){
            if(!dataModel::createOrUpdate($start, $end, $count, dataModel::STATUS_SUCCESS)){
                $this->log('Error!!! Not saved info about sync data. ' . $start->format('d.m.Y H:i:s') . ' ' . $end->format('d.m.Y H:i:s'));
                $this->addError('Error!!! Not saved info about sync data. ' . $start->format('d.m.Y H:i:s') . ' ' . $end->format('d.m.Y H:i:s'));
            } else {
                return true;
            }
        } else {
            App\Models\dataModel::createOrUpdate($start, $end, 0, dataModel::STATUS_NEW, 0, $resultSync);
            $errors = Func::getFromArray('ErrorList', $resultSync);
            $this->log('Errors  ' . getmypid());
            $this->logFile($errors, 'payment_errors.log');
            $this->addError('Payment errors: <br>' . print_r($errors, 1));
            unset($errors);
        }
        return false;
    }


    /**
     * @param DateTime $start
     * @param DateTime $end
     * @return bool
     */
    protected function getdata(DateTime $start, DateTime $end)
    {
        $key = md5($start->format('Y-m-d\TH:i:s') . $end->format('Y-m-d\TH:i:s'));
        $data = $this->cache->get($key);
        if(!$data) {
            $uid = $this->getUid();
            $params = [
                'getHourdataParameter' => [
                    'StartTime' => $start->format('Y-m-d\TH:i:s'),
                    'EndTime'   => $end->format('Y-m-d\TH:i:s'),
                    'RequestId' => $uid,
                    'Sign' => sha1($this->port_key . $uid)
                ]
            ];
            $data = $this->request('GetHourdata', $params);
            $this->cache->set($key, $data, 3600 * 3);
        }
        return $data;
    }


    /**
     * @param $payment
     * @return array
     */
    protected function createPayObject($payment)
    {
        $extra = isset($payment->ExtraPayment) && !empty($payment->Extra) ? $payment->Extra : 0;
        return [
            'subProviderId' =>  '12_' . $extra,
            'TranID' => $payment->PaymentId,
            'Merch' => 0,
            'GatewayId' => $payment->ProviderId
        ];
    }


    /**
     * @param $last_sync_time DateTime
     * @return bool
     */
    protected function checkTime($last_sync_time = null)
    {
        if(empty($last_sync_time)) {
            $last_sync_time = new DateTime();
            $last_sync_time->sub(new \DateInterval('PT' . (time() - 37000)  . 'M'));
        }
        return $last_sync_time->format('U') < (time() - 600);
    }


    /**
     * @param int $s
     */
    protected function sleep($s = 900)
    {
        if ($this->checkSem('stop-sync-data.txt') || !$this->active) {
            $this->log('Sync data is stopped');
            $this->afterDie();
            exit(0);
        }
        pcntl_signal_dispatch();
        $m = floor($s/60);
        echo date('Y-m-d H:i:s') . "--- Sleep --- {$m} minutes ---" . PHP_EOL;
        sleep($s);
        pcntl_signal_dispatch();
    }


    /**
     * Разбиваем время на промежутки
     *
     * @param DateTime $startTime
     * @param DateTime $endTime
     * @return array
     */
    protected function timeChunk(DateTime $startTime, DateTime $endTime)
    {
        $this->offset += 1;
        $parts = [];
        //промежутки требующие повторной синхронизации
        $withErrors = $this->syncErrors();
        $end = clone $startTime;
        $end->add(new \DateInterval('PT' . $this->offset  . 'M'));
        //$startTime->add(new \DateInterval('PT1M'));
        while($startTime < $endTime){
            $p = [
                'start_time' => clone $startTime,
                'end_time' => clone $end,
            ];
            dataModel::createOrUpdate($p['start_time'], $p['end_time']);
            $parts[] = $p;
            $startTime->add(new \DateInterval('PT' . $this->offset . 'M'));
            $end->add(new \DateInterval('PT' . $this->offset . 'M'));
        }
        $this->offset -= 1;

        //синхронизируем те промежутки времени которые были не синхронизированны
        $parts = array_merge_recursive($parts, $withErrors);
        if(empty($parts)){
            return [];
        }

        $countPart = round(count($parts)/$this->countWorkers);
        $countPart = $countPart > 0 ? $countPart : 1;
        return array_chunk($parts, $countPart);
    }


    /**
     * Получаем промежутки времени которые не были синхронизированны из-за ошибок
     *
     * @return array
     */
    protected function syncErrors()
    {
        $retry = [];
        foreach(dataModel::getWithErrors(4) as $errorSync){
            $retry[] =[
                'start_time' => new DateTime($errorSync['start_time']),
                'end_time' => new DateTime($errorSync['end_time'])
            ];
        }
        return $retry;
    }


    public function signalHandler($signo, $pid = null, $status = null) {
        switch($signo) {
            case SIGTERM:
                $this->stopSync();
                break;
            case SIGINT:
                $this->stopSync();
                break;
            default:
                $this->log('Signal' . $signo . ' does not have handlers');
        }
    }


    public function signalMainHandler($signo, $pid = null, $status = null) {
        $this->log('Stopping main ' . getmypid());
        switch($signo) {
            case SIGTERM:
                $this->stopMainSync();
                break;
            case SIGINT:
                $this->stopMainSync();
                break;
            default:
                $this->log('Signal' . $signo . ' does not has handlers');
        }
    }


    /**
     * Останавливаем основной поток
     * и посылаем сигналы дочерним
     */
    protected function stopMainSync()
    {
        if($this->active){
            $this->active = false;
            $this->log('Stopping main sync...');
            $this->killWorkers();
        }
    }


    protected function killWorkers()
    {
        foreach($this->workers as $workerPid){
            posix_kill($workerPid , SIGINT);
        }
    }


    /**
     * Завершаем воркер
     */
    protected function stopSync()
    {
        if($this->active){
            $this->log('Stopping worker ' . getmypid());
            $this->active = false;
        }
    }


    protected function setSignalHandlers($nameMethod)
    {
        if($this->setSignalHandler) {
            return true;
        }
        if(!function_exists('pcntl_signal_dispatch')){
            die('Not installed function pcntl_signal_dispatch');
        }
        if(!function_exists('pcntl_signal')){
            die('Not installed function pcntl_signal');
        }
        if(
            pcntl_signal(SIGTERM, [$this, $nameMethod]) &&
            pcntl_signal(SIGINT, [$this, $nameMethod])
        ) {
            return true;
        } else {
            die('Not set handler pcntl_signal');
        }
    }

}