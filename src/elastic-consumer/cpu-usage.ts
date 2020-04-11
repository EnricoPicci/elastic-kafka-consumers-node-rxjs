import { cpuUsage } from 'process';
import { of, interval } from 'rxjs';
import { tap, switchMap, map } from 'rxjs/operators';

let startDate: number;
let previousUsage: NodeJS.CpuUsage;
export function cpuUsageStream(frequency = 2000) {
    return of(Date.now()).pipe(
        tap((now) => {
            startDate = now;
            previousUsage = cpuUsage();
        }),
        switchMap(() => interval(frequency)),
        map(() => {
            const endDate = Date.now();
            const usage = cpuUsage(previousUsage);
            const result = (100 * (usage.user + usage.system)) / ((Date.now() - startDate) * 1000);
            startDate = endDate;
            previousUsage = cpuUsage();
            return result;
        }),
    );
}
