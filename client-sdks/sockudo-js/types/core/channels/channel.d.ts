import { default as EventsDispatcher } from '../events/dispatcher';
import Pusher from '../sockudo';
import { PusherEvent } from '../connection/protocol/message-types';
import { ChannelAuthorizationCallback } from '../auth/options';
import { FilterNode } from './filter';
export type SubscriptionRewind = number | {
    count?: number;
    seconds?: number;
};
export interface ChannelSubscriptionOptions {
    filter?: any;
    delta?: {
        enabled?: boolean;
        algorithm?: 'fossil' | 'xdelta3';
    };
    events?: string[];
    rewind?: SubscriptionRewind;
    annotationSubscribe?: boolean;
}
export interface PublishAnnotationRequest {
    type: string;
    name?: string;
    clientId?: string;
    socketId?: string;
    count?: number;
    data?: unknown;
    encoding?: string | null;
}
export interface PublishAnnotationResponse {
    annotationSerial: string;
}
export interface DeleteAnnotationResponse {
    annotationSerial: string;
    deletedAnnotationSerial: string;
}
export interface AnnotationEventsParams {
    type?: string;
    limit?: number;
    fromSerial?: string;
    socketId?: string;
}
export interface AnnotationEvent {
    action: 'annotation.create' | 'annotation.delete';
    id?: string;
    serial: string;
    messageSerial: string;
    type: string;
    name?: string;
    clientId?: string;
    count?: number;
    data?: unknown;
    encoding?: string;
    timestamp: number;
}
export interface AnnotationEventsResponse {
    channel: string;
    messageSerial: string;
    limit: number;
    hasMore: boolean;
    nextCursor?: string | null;
    items: AnnotationEvent[];
    hasNext(): boolean;
    next(): Promise<AnnotationEventsResponse>;
}
export default class Channel extends EventsDispatcher {
    name: string;
    pusher: Pusher;
    subscribed: boolean;
    subscriptionPending: boolean;
    subscriptionCancelled: boolean;
    subscriptionCount: null;
    tagsFilter: FilterNode | null;
    eventsFilter: string[] | null;
    rewind: SubscriptionRewind | null;
    annotationSubscribe: boolean;
    constructor(name: string, pusher: Pusher);
    authorize(socketId: string, callback: ChannelAuthorizationCallback): void;
    trigger(event: string, data: any): boolean;
    disconnect(): void;
    handleEvent(event: PusherEvent): void;
    handleSubscriptionSucceededEvent(event: PusherEvent): void;
    handleSubscriptionCountEvent(event: PusherEvent): void;
    publishAnnotation(messageSerial: string, annotation: PublishAnnotationRequest): Promise<PublishAnnotationResponse>;
    deleteAnnotation(messageSerial: string, annotationSerial: string, socketId?: string): Promise<DeleteAnnotationResponse>;
    listAnnotations(messageSerial: string, params?: AnnotationEventsParams): Promise<AnnotationEventsResponse>;
    subscribe(): void;
    unsubscribe(): void;
    cancelSubscription(): void;
    reinstateSubscription(): void;
}
